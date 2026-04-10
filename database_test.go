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

package database_test

import (
	"database/sql"
	_ "embed"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdrn-org/go-database"
	"github.com/tdrn-org/go-database/memory"
	"github.com/tdrn-org/go-database/postgres"
	"github.com/tdrn-org/go-database/sqlite"
)

//go:embed testdata/schema.1.sql
var schema1Script []byte

func TestMemory(t *testing.T) {
	config := memory.NewConfig(memory.WithSchemaScripts(schema1Script))
	testDatabase(t, config)
}

func TestSQLite(t *testing.T) {
	tempDir := t.TempDir()
	config := sqlite.NewConfig(filepath.Join(tempDir, "test.db"), sqlite.WithSchemaScripts(schema1Script))
	testDatabase(t, config)
}

func TestPostgres(t *testing.T) {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	if host == "" || port == "" {
		t.Skip("PostgreSQL not available")
	}
	address := host + ":" + port
	config := postgres.NewConfig("postgres", "postgres", "postgres", postgres.WithAddress(address), postgres.WithSchemaScripts(schema1Script))
	testDatabase(t, config)
}

func testDatabase(t *testing.T, c database.Config) {
	const insertValueSQL string = "INSERT INTO value(id,value) VALUES($1,$2)"
	const selectValueSQL string = "SELECT value FROM value WHERE id=$1"
	// Open
	db, err := database.Open(c)
	require.NoError(t, err)

	// Ping
	require.NoError(t, db.Ping(t.Context()))

	// Update schema
	target := database.Schema(1)
	from, to, err := db.UpdateSchema(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, database.SchemaNone, from)
	require.Equal(t, target, to)

	// Commit
	var commitId string
	{
		txCtx, tx, err := db.BeginTx(t.Context())
		require.NoError(t, err)
		commitId = database.NewID()
		err = tx.ExecTx(txCtx, insertValueSQL, commitId, t.Name())
		require.NoError(t, err)
		err = tx.CommitTx(txCtx)
		require.NoError(t, err)
		require.NoError(t, tx.CloseTx(txCtx))
	}
	{
		txCtx, tx, err := db.BeginTx(t.Context())
		require.NoError(t, err)
		row, err := tx.QueryRowTx(txCtx, selectValueSQL, commitId)
		require.NoError(t, err)
		var value string
		err = row.Scan(&value)
		require.NoError(t, err)
		require.Equal(t, t.Name(), value)
		require.NoError(t, tx.CloseTx(txCtx))
	}
	{
		txCtx, tx, err := db.BeginTx(t.Context())
		require.NoError(t, err)
		rows, err := tx.QueryTx(txCtx, selectValueSQL, commitId)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var value string
		err = rows.Scan(&value)
		require.NoError(t, err)
		require.Equal(t, t.Name(), value)
		require.False(t, rows.Next())
		require.NoError(t, tx.CloseTx(txCtx))
	}

	// Rollback
	var rollbackId string
	{
		txCtx, tx, err := db.BeginTx(t.Context())
		require.NoError(t, err)
		rollbackId = database.NewID()
		err = tx.ExecTx(txCtx, insertValueSQL, rollbackId, t.Name())
		require.NoError(t, err)
		require.NoError(t, tx.CloseTx(txCtx))
	}
	{
		txCtx, tx, err := db.BeginTx(t.Context())
		require.NoError(t, err)
		row, err := tx.QueryRowTx(txCtx, selectValueSQL, rollbackId)
		require.NoError(t, err)
		var value string
		err = row.Scan(&value)
		require.True(t, database.NoRows(err))
		require.NoError(t, tx.CloseTx(txCtx))
	}
	{
		txCtx, tx, err := db.BeginTx(t.Context())
		require.NoError(t, err)
		rows, err := tx.QueryTx(txCtx, selectValueSQL, rollbackId)
		require.NoError(t, err)
		require.False(t, rows.Next())
		require.NoError(t, tx.CloseTx(txCtx))
	}

	// Close
	require.NoError(t, db.Close())
}

func TestNewID(t *testing.T) {
	id1 := database.NewID()
	require.NotEmpty(t, id1)
	id2 := database.NewID()
	require.NotEqual(t, id1, id2)
}

func TestTime2DB2Time(t *testing.T) {
	now := database.Now()
	timeValue := database.DB2Time(now)
	databaseValue := database.Time2DB(timeValue)
	require.Equal(t, now, databaseValue)
}

func TestNoRows(t *testing.T) {
	require.True(t, database.NoRows(sql.ErrNoRows))
	require.False(t, database.NoRows(sql.ErrTxDone))
}
