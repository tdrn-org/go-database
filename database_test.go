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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdrn-org/go-database"
	"github.com/tdrn-org/go-database/memory"
	"github.com/tdrn-org/go-database/postgres"
	"github.com/tdrn-org/go-database/sqlite"
)

func TestMemory(t *testing.T) {
	config := memory.NewConfig()
	testDatabase(t, config)
}

func TestSQLite(t *testing.T) {
	tempDir := t.TempDir()
	config := sqlite.NewConfig(filepath.Join(tempDir, "test.db"))
	testDatabase(t, config)
}

func TestPostgres(t *testing.T) {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	if host == "" || port == "" {
		t.Skip("PostgreSQL not available")
	}
	address := host + ":" + port
	config := postgres.NewConfig("postgres", "postgres", "postgres", postgres.WithAddress(address))
	testDatabase(t, config)
}

func testDatabase(t0 *testing.T, c database.Config) {
	t0.Run("openPingClose", func(t *testing.T) {
		db, err := database.Open(c)
		require.NoError(t, err)
		require.NoError(t, db.Ping(t.Context()))
		require.NoError(t, db.Close())
	})
	t0.Run("updateSchema", func(t *testing.T) {
		db, err := database.Open(c)
		require.NoError(t, err)
		from, to, err := db.UpdateSchema(t.Context(), database.Schema0)
		require.NoError(t, err)
		require.Equal(t, database.SchemaNone, from)
		require.Equal(t, database.Schema0, to)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})
}
