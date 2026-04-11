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

// Package sqlite provides a PostgreSQL based database configuration.
package postgres

import (
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tdrn-org/go-database"
	"github.com/tdrn-org/go-tlsconf/tlsclient"
)

// Type of PostgreSQL database configurations.
const Type database.Type = "postgres"

// Config represents the PostgreSQL database configuration.
type Config struct {
	address       string
	dbName        string
	user          string
	password      string
	schemaScripts [][]byte
}

//go:embed schema.0.sql
var schema0Script []byte

// NewConfig creates a new PostgreSQL database configuration using the given options.
func NewConfig(dbName, user, password string, options ...ConfigSetter) *Config {
	config := &Config{
		address:       "localhost:5432",
		dbName:        dbName,
		user:          user,
		password:      password,
		schemaScripts: [][]byte{schema0Script},
	}
	for _, option := range options {
		option.Apply(config)
	}
	return config
}

// Name gets the name of the database represented by this configuration.
func (c *Config) Name() string {
	return fmt.Sprintf("%s@%s", c.dbName, c.address)
}

// Type gets the database type represented by this configuration.
func (c *Config) Type() database.Type {
	return Type
}

// DriverName gets the name of the sql driver providing access to the database
// represented by this configuration.
func (c *Config) DriverName() string {
	return "pgx"
}

// DSN get the Data Source Name to be used for accessing the database.
func (c *Config) DSN() string {
	connString := fmt.Sprintf("postgres://%s:%s@%s/%s", c.user, c.password, c.address, c.dbName)
	connConfig, _ := pgx.ParseConfig(connString)
	connConfig.TLSConfig = tlsclient.GetConfig().Clone()
	connConfig.TLSConfig.ServerName = connConfig.Host
	return stdlib.RegisterConnConfig(connConfig)
}

// DSN get the Data Source Name with any sensitive data redacted.
func (c *Config) RedactedDSN() string {
	return fmt.Sprintf("postgres://%s:***@%s/%s", c.user, c.address, c.dbName)
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

// WithAddress sets the address string to use to connect to
// the PostgreSQL database.
func WithAddress(address string) ConfigSetter {
	return ConfigSetterFunc(func(c *Config) {
		c.address = address
	})
}
