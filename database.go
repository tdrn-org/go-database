/*
 * Copyright 2025-2026 Holger de Carne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Schema int

const (
	SchemaNone Schema = -1
	Schema0    Schema = 0
)

type Name string

func (name Name) String() string {
	return string(name)
}

type Config interface {
	Name() Name
	DriverName() string
	DSN() string
	RedactedDSN() string
	SchemaScripts() [][]byte
}

type Driver struct {
	config        Config
	db            *sql.DB
	preparedStmts map[string]*sql.Stmt
	schemaScripts [][]byte
	logger        *slog.Logger
	tracer        trace.Tracer
	mutex         sync.RWMutex
}

func Open(config Config) (*Driver, error) {
	name := config.Name()
	logger := slog.With(slog.Any("database", name))
	logger.Debug("opening database", slog.String("dsn", config.RedactedDSN()))
	db, err := sql.Open(config.DriverName(), config.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed open %s database (cause: %w)", name, err)
	}
	driver := &Driver{
		config:        config,
		db:            db,
		preparedStmts: make(map[string]*sql.Stmt),
		schemaScripts: config.SchemaScripts(),
		logger:        logger,
		tracer:        otel.Tracer(reflect.TypeFor[Driver]().PkgPath(), trace.WithInstrumentationAttributes(attribute.Stringer("database", name))),
	}
	return driver, nil
}

func (d *Driver) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}

func (d *Driver) Close() error {
	d.logger.Debug("closing database")
	return d.db.Close()
}

var txPool sync.Pool = sync.Pool{
	New: func() any {
		return &Tx{}
	},
}

type Tx struct {
	outerTx   *Tx
	committed bool
	driver    *Driver
	sqlTx     *sql.Tx
	ctx       context.Context
	now       time.Time
	span      trace.Span
}

func (d *Driver) BeginTx(ctx context.Context) (context.Context, *Tx, error) {
	outerTx, nestedTx := ctx.Value(d).(*Tx)
	if nestedTx {
		tx := txPool.Get().(*Tx)
		tx.outerTx = outerTx
		tx.driver = d
		tx.sqlTx = outerTx.sqlTx
		tx.ctx = ctx
		tx.now = outerTx.now
		return ctx, tx, nil
	}
	traceCtx, span := d.tracer.Start(ctx, "Tx", trace.WithSpanKind(trace.SpanKindInternal))
	sqlTx, err := d.beginTx(traceCtx)
	if err != nil {
		traceError(span, err)
		span.End()
		return nil, nil, fmt.Errorf("begin transaction failure (cause: %w)", err)
	}
	tx := txPool.Get().(*Tx)
	tx.driver = d
	tx.sqlTx = sqlTx
	tx.ctx = context.WithValue(traceCtx, d, tx)
	tx.now = time.Now().UTC()
	tx.span = span
	return tx.ctx, tx, nil
}

func (tx *Tx) Now() time.Time {
	return tx.now
}

func (tx *Tx) ExecTx(ctx context.Context, query string, args ...any) error {
	return tx.driver.execTx(ctx, tx.sqlTx, query, args...)
}

func (tx *Tx) QueryRowTx(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	return tx.driver.queryRowTx(ctx, tx.sqlTx, query, args...)
}

func (tx *Tx) QueryTx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.driver.queryTx(ctx, tx.sqlTx, query, args...)
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	tx.committed = true
	if tx.outerTx != nil {
		return nil
	}
	err := tx.driver.commitTx(ctx, tx.sqlTx)
	if err != nil {
		traceError(tx.span, err)
		return fmt.Errorf("commit failure (cause: %w)", err)
	}
	return nil
}

func (tx *Tx) Close() {
	if tx == nil {
		return
	}
	if !tx.committed {
		err := tx.driver.rollbackTx(tx.ctx, tx.sqlTx)
		if err != nil {
			traceError(tx.span, err)
		}
	}
	if tx.outerTx == nil {
		tx.span.End()
	}
	tx.outerTx = nil
	tx.committed = false
	tx.driver = nil
	tx.sqlTx = nil
	tx.ctx = nil
	tx.now = time.Time{}
	tx.span = nil
	txPool.Put(tx)
}

func (d *Driver) beginTx(ctx context.Context) (*sql.Tx, error) {
	traceCtx, span := d.tracer.Start(ctx, "BeginTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	tx, err := d.db.BeginTx(traceCtx, nil)
	if err != nil {
		traceError(span, err)
		return nil, fmt.Errorf("begin transaction failure (cause: %w)", err)
	}
	return tx, nil
}

func (d *Driver) commitTx(ctx context.Context, tx *sql.Tx) error {
	_, span := d.tracer.Start(ctx, "CommitTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	err := tx.Commit()
	if err != nil {
		traceError(span, err)
		return fmt.Errorf("commit failure (cause: %w)", err)
	}
	return nil
}

func (d *Driver) rollbackTx(ctx context.Context, tx *sql.Tx) error {
	_, span := d.tracer.Start(ctx, "RollbackTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		err = nil
	} else if err != nil {
		d.logger.Error("rollback failure", slog.Any("err", err))
	}
	return err
}

func (d *Driver) execTx(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	traceCtx, span := d.tracer.Start(ctx, "ExecTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	d.logger.Debug("sql exec", slog.String("query", query))
	stmt, err := d.prepareStmt(traceCtx, query)
	if err != nil {
		return traceError(span, err)
	}
	txStmt := tx.StmtContext(traceCtx, stmt)
	result, err := txStmt.ExecContext(traceCtx, args...)
	if err != nil {
		return traceError(span, fmt.Errorf("sql exec failure: '%s' (cause: %w)", query, err))
	}
	rows, err := result.RowsAffected()
	if err == nil {
		d.logger.Debug("sql exec complete", slog.Int64("rows", rows))
	}
	return nil
}

func (d *Driver) queryRowTx(ctx context.Context, tx *sql.Tx, query string, args ...any) (*sql.Row, error) {
	traceCtx, span := d.tracer.Start(ctx, "QueryRowTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	d.logger.Debug("sql query row", slog.String("query", query))
	stmt, err := d.prepareStmt(traceCtx, query)
	if err != nil {
		return nil, traceError(span, err)
	}
	txStmt := tx.StmtContext(traceCtx, stmt)
	return txStmt.QueryRowContext(traceCtx, args...), nil
}

func (d *Driver) queryTx(ctx context.Context, tx *sql.Tx, query string, args ...any) (*sql.Rows, error) {
	traceCtx, span := d.tracer.Start(ctx, "QueryTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	d.logger.Debug("sql query", slog.String("query", query))
	stmt, err := d.prepareStmt(traceCtx, query)
	if err != nil {
		return nil, traceError(span, err)
	}
	txStmt := tx.StmtContext(traceCtx, stmt)
	rows, err := txStmt.QueryContext(traceCtx, args...)
	if err != nil {
		return nil, traceError(span, fmt.Errorf("sql query failure: '%s' (cause: %w)", query, err))
	}
	return rows, nil
}

func (d *Driver) prepareStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	d.mutex.RLock()
	stmt := d.preparedStmts[query]
	d.mutex.RUnlock()
	if stmt != nil {
		return stmt, nil
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	stmt, err := d.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: '%s' (cause: %w)", query, err)
	}
	d.preparedStmts[query] = stmt
	return stmt, nil
}

func (d *Driver) UpdateSchema(ctx context.Context, target Schema) (Schema, Schema, error) {
	traceCtx, span := d.tracer.Start(ctx, "UpdateSchema", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	// Determine schema version (assuming none, in case of error)
	tx, err := d.beginTx(traceCtx)
	if err != nil {
		return SchemaNone, SchemaNone, traceError(span, err)
	}
	fromVersion, _ := d.querySchemaVersion(traceCtx, tx)
	d.rollbackTx(traceCtx, tx)

	// Update schema as needed
	if fromVersion == target {
		d.logger.Debug("database up-to-date", slog.Int("schema", int(fromVersion)))
	}
	currentVersion := fromVersion
	for currentVersion != target {
		tx, err = d.beginTx(traceCtx)
		if err != nil {
			return fromVersion, currentVersion, traceError(span, err)
		}
		nextVersion := currentVersion + 1
		d.logger.Info("updating database schema", slog.Int("from", int(currentVersion)), slog.Int("to", int(nextVersion)))
		if 0 <= nextVersion && nextVersion < Schema(len(d.schemaScripts)) {
			err = d.scriptTx(traceCtx, tx, d.schemaScripts[nextVersion])
		} else {
			err = fmt.Errorf("unrecognized database schema version: %d", currentVersion)
		}
		if err != nil {
			err = fmt.Errorf("database schema update failure (cause: %w)", err)
			return fromVersion, currentVersion, traceError(span, errors.Join(err, d.rollbackTx(traceCtx, tx)))
		}
		err = d.commitTx(traceCtx, tx)
		if err != nil {
			err = fmt.Errorf("database schema commit failure (cause: %w)", err)
			return fromVersion, currentVersion, traceError(span, errors.Join(err, d.rollbackTx(traceCtx, tx)))
		}
		currentVersion = nextVersion
	}
	return fromVersion, currentVersion, nil
}

func (d *Driver) querySchemaVersion(ctx context.Context, tx *sql.Tx) (Schema, error) {
	row, err := d.queryRowTx(ctx, tx, "SELECT schema FROM version")
	if err != nil {
		return SchemaNone, err
	}
	var schema Schema
	err = row.Scan(&schema)
	if err != nil {
		return SchemaNone, err
	}
	return schema, nil
}

func (d *Driver) scriptTx(ctx context.Context, tx *sql.Tx, script []byte) error {
	traceCtx, span := d.tracer.Start(ctx, "ScriptTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	reader, err := newSQLScriptReader(script)
	if err != nil {
		return err
	}
	for {
		statement, err := reader.readStatement()
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		err = d.scriptExecTx(traceCtx, tx, statement)
		if err != nil {
			return fmt.Errorf("script exec failure at %d (cause: %w)", reader.LineNo(), err)
		}
	}
}

func (d *Driver) scriptExecTx(ctx context.Context, tx *sql.Tx, statement string) error {
	traceCtx, span := d.tracer.Start(ctx, "ScriptExecTx", trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	d.logger.Debug("sql script exec", slog.String("statement", statement))
	result, err := tx.ExecContext(traceCtx, statement)
	if err != nil {
		return traceError(span, fmt.Errorf("sql script exec failure: '%s' (cause: %w)", statement, err))
	}
	rows, err := result.RowsAffected()
	if err == nil {
		d.logger.Debug("sql script exec complete", slog.Int64("rows", rows))
	}
	return nil
}

func NewID() string {
	return uuid.NewString()
}

func Now() int64 {
	return Time2DB(time.Now().UTC())
}

func Time2DB(t time.Time) int64 {
	return t.UnixMicro()
}

func DB2Time(msec int64) time.Time {
	return time.UnixMicro(msec)
}

func DB2JSONTime(msec int64) string {
	return DB2Time(msec).Format(time.RFC3339)
}

func NoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func traceError(span trace.Span, err error) error {
	if err != nil {
		span.RecordError(err)
		_, file, line, _ := runtime.Caller(2)
		baseFile := filepath.Base(file)
		lineNo := strconv.Itoa(line)
		source := baseFile + ":" + lineNo
		span.SetStatus(codes.Error, source+" "+err.Error())
	}
	return err
}
