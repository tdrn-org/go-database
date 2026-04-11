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
	"maps"
	"slices"
	"strings"

	"github.com/tdrn-org/go-database"
	_ "modernc.org/sqlite"
)

// Type of SQLite3 database configurations.
const Type database.Type = "sqlite"

// SQLite3 open modes
type Mode string

const (
	// ModeRWC opens database for read and write operation and creates it if not yet existent.
	ModeRWC Mode = "rwc"
	// ModeRW opens database for read and write operation.
	ModeRW Mode = "rw"
	// ModeRO opens database for read operations only.
	ModeRO Mode = "ro"
	// ModeMemory opens a memory based database.
	ModeMemory Mode = "memory"
)

// Config represents the SQLite3 database configuration.
type Config struct {
	file          string
	mode          Mode
	options       map[string]string
	schemaScripts [][]byte
}

//go:embed schema.0.sql
var schema0Script []byte

var defaultOptions map[string]string = map[string]string{
	"cache":         "shared",
	"_foreign_keys": "on",
	"_locking":      "EXCLUSIVE",
	"_journal":      "WAL",
}

// NewConfig creates a new SQLite3 database configuration using the given options.
func NewConfig(file string, mode Mode, options ...ConfigSetter) *Config {
	config := &Config{
		file:          file,
		mode:          mode,
		options:       make(map[string]string),
		schemaScripts: [][]byte{schema0Script},
	}
	for _, option := range options {
		option.Apply(config)
	}
	if len(config.options) == 0 {
		maps.Copy(config.options, defaultOptions)
	}
	return config
}

// Name gets the name of the database represented by this configuration.
func (c *Config) Name() string {
	return c.file
}

// Type gets the database type represented by this configuration.
func (c *Config) Type() database.Type {
	return Type
}

// DriverName gets the name of the sql driver providing access to the database
// represented by this configuration.
func (c *Config) DriverName() string {
	return "sqlite"
}

// DSN get the Data Source Name to be used for accessing the database.
func (c *Config) DSN() string {
	dsn := strings.Builder{}
	dsn.WriteString(fmt.Sprintf("file:%s?mode=%s", c.file, c.mode))
	sortedKeys := slices.Collect(maps.Keys(c.options))
	slices.Sort(sortedKeys)
	for _, key := range sortedKeys {
		dsn.WriteString(fmt.Sprintf("&%s=%s", key, c.options[key]))
	}
	return dsn.String()
}

// DSN get the Data Source Name with any sensitive data redacted.
func (c *Config) RedactedDSN() string {
	return c.DSN()
}

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

// WithOption adds a SQLite specific option to the DSN.
func WithOption(key, value string) ConfigSetter {
	return ConfigSetterFunc(func(c *Config) {
		c.options[key] = value
	})
}

// WithOptions adds SQLite specific options to the DSN.
func WithOptions(options map[string]string) ConfigSetter {
	return ConfigSetterFunc(func(c *Config) {
		maps.Copy(c.options, options)
	})
}
