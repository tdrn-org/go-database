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

// Package database provides a simple layer on top of database/sql to
// ease common database tasks like transaction handling, prepared
// statement caching, schema updates and the like.
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

// Schema version type.
type Schema int

const (
	// SchemaNone indicates no schema has been setup so far.
	SchemaNone Schema = -1
	// Schema0 indicates the basic schema is in place.
	Schema0 Schema = 0
)

// Type represents the database type.
type Type string

func (name Type) String() string {
	return string(name)
}

// Config interface provides a generic way to different
// database drivers.
type Config interface {
	// Type gets the database type represented by this configuration.
	Type() Type
	// DriverName gets the name of the sql driver providing access to the database
	// represented by this configuration.
	DriverName() string
	// DSN get the Data Source Name to be used for accessing the database.
	DSN() string
	// DSN get the Data Source Name with any sensitive data redacted.
	RedactedDSN() string
	// SchemaScripts gets the schema updated scripts to be applied to the database
	// during schema initialization or a schema update.
	SchemaScripts() [][]byte
}

// Driver represents an open database connection ready to execute SQL statements.
type Driver struct {
	config        Config
	db            *sql.DB
	preparedStmts map[string]*sql.Stmt
	schemaScripts [][]byte
	logger        *slog.Logger
	tracer        trace.Tracer
	mutex         sync.RWMutex
}

// Open opens the database represented by the given [Config] instance.
func Open(config Config) (*Driver, error) {
	databaseType := config.Type()
	logger := slog.With(slog.Any("database", databaseType), slog.String("dsn", config.RedactedDSN()))
	logger.Info("opening database")
	db, err := sql.Open(config.DriverName(), config.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open %s database (cause: %w)", databaseType, err)
	}
	driver := &Driver{
		config:        config,
		db:            db,
		preparedStmts: make(map[string]*sql.Stmt),
		schemaScripts: config.SchemaScripts(),
		logger:        logger,
		tracer:        otel.Tracer(reflect.TypeFor[Driver]().PkgPath(), trace.WithInstrumentationAttributes(attribute.Stringer("database", databaseType), attribute.String("dsn", config.RedactedDSN()))),
	}
	return driver, nil
}

// Ping pings the database (see [sql.DB.PingContext]).
func (d *Driver) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}

// Close closes the database connection and all related resources.
func (d *Driver) Close() error {
	d.logger.Info("closing database")
	closeErrs := make([]error, 1+len(d.preparedStmts))
	closeErrs = append(closeErrs, d.db.Close())
	for _, preparedStmt := range d.preparedStmts {
		closeErrs = append(closeErrs, preparedStmt.Close())
	}
	return errors.Join(closeErrs...)
}

var txPool sync.Pool = sync.Pool{
	New: func() any {
		return &Tx{}
	},
}

// Tx represents a database transation (see [BeginTx]).
type Tx struct {
	outerTx   *Tx
	committed bool
	driver    *Driver
	sqlTx     *sql.Tx
	now       time.Time
	span      trace.Span
}

// BeginTx ensures a database transaction is in place.
//
// Make sure to always close the returned Tx instance by invoking[Tx.CloseTx].
//
// Invoke [Tx.CommitTx] to commit all database updates performed within the BeginTx
// block. If [Tx.CommitTx] is not called before the call to [Tx.CloseTx], it will
// be implicitly rolled back.
//
// In case of an outer to BeginTx call for the identical context, the already
// opened transaction is re-used. Closing the transaction behaves in the
// same manner. Only after the last BeginTx block is closed by invoking
// [Tx.CloseTx] and only if all BeginTx blocks have been commited by invoking
// [Tx.CommitTx] the actual database transaction is committed.
func (d *Driver) BeginTx(ctx context.Context) (context.Context, *Tx, error) {
	outerTx, nestedTx := ctx.Value(d).(*Tx)
	if nestedTx {
		tx := txPool.Get().(*Tx)
		tx.outerTx = outerTx
		tx.driver = d
		tx.sqlTx = outerTx.sqlTx
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
	tx.now = time.Now().UTC()
	tx.span = span
	txCtx := context.WithValue(traceCtx, d, tx)
	return txCtx, tx, nil
}

// Now returns the time the transaction was started, hence returning a consistent
// time value across the whole transaction lifecycle.
func (tx *Tx) Now() time.Time {
	return tx.now
}

// ExecTx executes a SQL statement (see [sql.DB.ExecContext]).
func (tx *Tx) ExecTx(ctx context.Context, query string, args ...any) error {
	return tx.driver.execTx(ctx, tx.sqlTx, query, args...)
}

// QueryRowTx queries a single row (see [sql.DB.QueryRowContext]).
func (tx *Tx) QueryRowTx(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	return tx.driver.queryRowTx(ctx, tx.sqlTx, query, args...)
}

// QueryTx executes a database query (see [sql.DB.QueryContext]).
func (tx *Tx) QueryTx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.driver.queryTx(ctx, tx.sqlTx, query, args...)
}

// Commit commits all changes since the corresponding [Driver.BeginTx] call.
// See [Driver.BeginTx] and [Tx.CloseTx] for details of the database transaction
// lifecycle.
func (tx *Tx) CommitTx(ctx context.Context) error {
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

// Close closes the transaction returned by [Driver.BeginTx].
//
// If [Tx.CommitTx] has not been invoked for the transaction, the
// transaction is implicitly rolled back.
func (tx *Tx) CloseTx(ctx context.Context) error {
	if tx == nil {
		return nil
	}
	var err error
	if !tx.committed {
		err = tx.driver.rollbackTx(ctx, tx.sqlTx)
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
	tx.now = time.Time{}
	tx.span = nil
	txPool.Put(tx)
	return err
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

// UpdateSchema is used to update the database schema to the given target schema version.
//
// The current schema version is determined by querying the database. Any necessary schema
// update is performed by getting the update scripts via [Config.SchemaScripts] and executing
// them as needed.
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
		statement, err := reader.ReadStatement()
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

// NewID generates a id suitable for use as a primary key.
func NewID() string {
	return uuid.NewString()
}

// Now gets the current time as database compatible int64 type and based on UTC timezone.
func Now() int64 {
	return Time2DB(time.Now().UTC())
}

// Time2DB converts the given [time.Time] value to the corresponding database time value.
func Time2DB(t time.Time) int64 {
	return t.UnixMicro()
}

// DB2Time converts the given database time value to the corresponding [time.Time] value.
func DB2Time(msec int64) time.Time {
	return time.UnixMicro(msec)
}

// NoRows checks whether the given error is [sql.ErrNoRows].
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
