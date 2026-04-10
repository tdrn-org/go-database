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

// Package sqlite provides a SQLite3 based database configuration.
package sqlite

import (
	_ "embed"
	"fmt"

	"github.com/tdrn-org/go-database"
	_ "modernc.org/sqlite"
)

// Name of SQLite3 database configuration.
const Name database.Name = "sqlite"

// Config represents the SQLite3 database configuration.
type Config struct {
	name          database.Name
	file          string
	schemaScripts [][]byte
}

// NewConfig creates a new SQLite3 database configuration using the given options.
func NewConfig(file string, options ...ConfigSetter) *Config {
	config := &Config{
		name:          Name,
		file:          file,
		schemaScripts: [][]byte{schema0Script},
	}
	for _, option := range options {
		option.Apply(config)
	}
	return config
}

// Name gets the name of the database configuration.
func (c *Config) Name() database.Name {
	return c.name
}

// DriverName gets the name of the sql driver providing access to the database
// represented by this configuration.
func (c *Config) DriverName() string {
	return "sqlite"
}

const sqlite3DSNPattern = "file:%s?mode=%s&cache=shared&_foreign_keys=on&_locking=EXCLUSIVE&_journal=WAL"

// DSN get the Data Source Name to be used for accessing the database.
func (c *Config) DSN() string {
	mode := "rwc"
	return fmt.Sprintf(sqlite3DSNPattern, c.file, mode)
}

// DSN get the Data Source Name with any sensitive data redacted.
func (c *Config) RedactedDSN() string {
	return c.DSN()
}

//go:embed schema.0.sql
var schema0Script []byte

// SchemaScripts gets the schema updated scripts to be applied to the database
// during schema initialization or a schema update.
func (c *Config) SchemaScripts() [][]byte {
	return c.schemaScripts
}

// ConfigSetter interface is used to set database configuration options
// during a [NewConfig] call.
type ConfigSetter interface {
	Apply(*Config)
}

// ConfigSetterFunc functions are used to set database configuration options
// during a [NewConfig] call.
type ConfigSetterFunc func(*Config)

func (f ConfigSetterFunc) Apply(c *Config) {
	f(c)
}

// WithSchemaScripts defines the schema update scripts to be applied
// during database schema update.
func WithSchemaScripts(scripts ...[]byte) ConfigSetter {
	return ConfigSetterFunc(func(c *Config) {
		c.schemaScripts = append(c.schemaScripts, scripts...)
	})
}
