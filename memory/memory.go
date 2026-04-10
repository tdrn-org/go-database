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

package memory

import (
	_ "embed"
	"fmt"

	"github.com/tdrn-org/go-database"
	_ "modernc.org/sqlite"
)

const Name database.Name = "memory"

type Config struct {
	file          string
	schemaScripts [][]byte
}

func NewConfig(options ...ConfigSetter) *Config {
	config := &Config{
		file:          "idpd.db",
		schemaScripts: [][]byte{schema0Script},
	}
	for _, option := range options {
		option.Apply(config)
	}
	return config
}

func (c *Config) Name() database.Name {
	return Name
}

func (c *Config) DriverName() string {
	return "sqlite"
}

const sqlite3DSNPattern = "file:%s?mode=%s&cache=shared&_foreign_keys=on&_locking=EXCLUSIVE&_journal=WAL"

func (c *Config) DSN() string {
	mode := "memory"
	return fmt.Sprintf(sqlite3DSNPattern, c.file, mode)
}

func (c *Config) RedactedDSN() string {
	return c.DSN()
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
