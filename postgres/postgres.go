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

package postgres

import (
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tdrn-org/go-database"
	"github.com/tdrn-org/go-tlsconf/tlsclient"
)

const Name database.Name = "postgres"

type Config struct {
	name          database.Name
	address       string
	dbName        string
	user          string
	password      string
	schemaScripts [][]byte
}

func NewConfig(dbName, user, password string, options ...ConfigSetter) *Config {
	config := &Config{
		name:          Name,
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

func (c *Config) Name() database.Name {
	return c.name
}

func (c *Config) DriverName() string {
	return "pgx"
}

func (c *Config) DSN() string {
	connString := fmt.Sprintf("postgres://%s:%s@%s/%s", c.user, c.password, c.address, c.dbName)
	connConfig, _ := pgx.ParseConfig(connString)
	connConfig.TLSConfig = tlsclient.GetConfig().Clone()
	connConfig.TLSConfig.ServerName = connConfig.Host
	return stdlib.RegisterConnConfig(connConfig)
}

func (c *Config) RedactedDSN() string {
	return fmt.Sprintf("postgres://%s:***@%s/%s", c.user, c.address, c.dbName)
}

//go:embed schema.0.sql
var schema0Script []byte

func (c *Config) SchemaScripts() [][]byte {
	return c.schemaScripts
}

type ConfigSetter interface {
	Apply(*Config)
}

type ConfigSetterFunc func(*Config)

func (f ConfigSetterFunc) Apply(c *Config) {
	f(c)
}

func WithSchemaScripts(scripts ...[]byte) ConfigSetter {
	return ConfigSetterFunc(func(c *Config) {
		c.schemaScripts = append(c.schemaScripts, scripts...)
	})
}

func WithAddress(address string) ConfigSetter {
	return ConfigSetterFunc(func(c *Config) {
		c.address = address
	})
}
