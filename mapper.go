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
	"database/sql"
	"errors"
	"reflect"
)

// ScanRow scans the given row and maps the fetched columns to the
// struct fields based on their tagging. The given columns
// must match the order and names of the row result.
func ScanRow(row *sql.Row, dest any, columns ...string) error {
	args, err := scanArgs(dest, columns...)
	if err != nil {
		return err
	}
	return row.Scan(args...)
}

// Scan scans the current row and maps the fetched columns to the
// struct fields based on their tagging.
func Scan(rows *sql.Rows, dest any) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	args, err := scanArgs(dest, columns...)
	if err != nil {
		return err
	}
	return rows.Scan(args...)
}

func scanArgs(dest any, columns ...string) ([]any, error) {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Struct {
		return nil, errors.New("invalid scan target; must be pointer to struct")
	}
	structValue := destValue.Elem()
	structType := structValue.Type()
	numField := structValue.NumField()
	fieldMapping := make(map[string]int, numField)
	for i := 0; i < numField; i++ {
		field := structType.Field(i)
		tag := field.Tag.Get("db")
		if tag != "" && tag != "-" {
			fieldMapping[tag] = i
		}
	}
	args := make([]any, len(columns))
	var ignore any
	for i, column := range columns {
		fieldIndex, mapped := fieldMapping[column]
		if mapped {
			args[i] = structValue.Field(fieldIndex).Addr().Interface()
		} else {
			args[i] = &ignore
		}
	}
	return args, nil
}
