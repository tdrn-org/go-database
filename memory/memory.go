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

// Package memory provides a memory based database configuration (by actually
// providing a SQLite3 database with memory backend).
//
// Any data stored in this type of database will be lost after the
// database is closed.
package memory

import (
	_ "embed"

	"github.com/tdrn-org/go-database"
	"github.com/tdrn-org/go-database/sqlite"
	_ "modernc.org/sqlite"
)

// Type of memory database configurations.
const Type database.Type = "memory"

// Config represents the memory database configuration.
type Config struct {
	sqliteConfig database.Config
}

// NewConfig creates a new memory database configuration using the given options.
func NewConfig(options ...sqlite.ConfigSetter) *Config {
	return &Config{sqliteConfig: sqlite.NewConfig("memory.db", sqlite.ModeMemory, options...)}
}

// Name gets the name of the database represented by this configuration.
func (c *Config) Name() string {
	return Type.String()
}

// Type gets the database type represented by this configuration.
func (c *Config) Type() database.Type {
	return Type
}

// DriverName gets the name of the sql driver providing access to the database
// represented by this configuration.
func (c *Config) DriverName() string {
	return c.sqliteConfig.DriverName()
}

// DSN get the Data Source Name to be used for accessing the database.
func (c *Config) DSN() string {
	return c.sqliteConfig.DSN()
}

// DSN get the Data Source Name with any sensitive data redacted.
func (c *Config) RedactedDSN() string {
	return c.sqliteConfig.RedactedDSN()
}

// SchemaScripts gets the schema updated scripts to be applied to the database
// during schema initialization or a schema update.
func (c *Config) SchemaScripts() [][]byte {
	return c.sqliteConfig.SchemaScripts()
}
