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

package database

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

type sqlScriptReader struct {
	reader *bufio.Reader
	lineNo int
}

func newSQLScriptReader(script []byte) (*sqlScriptReader, error) {
	reader := bufio.NewReader(bytes.NewReader(script))
	r := &sqlScriptReader{
		reader: reader,
	}
	return r, nil
}

func (r *sqlScriptReader) LineNo() int {
	return r.lineNo
}

func (r *sqlScriptReader) readStatement() (string, error) {
	statement := &strings.Builder{}
	for {
		line, excessive, err := r.reader.ReadLine()
		if errors.Is(err, io.EOF) {
			if statement.Len() > 0 {
				return "", fmt.Errorf("incomplete statement at %d", r.lineNo)
			}
			return "", err
		} else if err != nil {
			return "", fmt.Errorf("failed to read script (cause: %w)", err)
		}
		r.lineNo++
		if excessive {
			return "", fmt.Errorf("excessive line length at %d", r.lineNo)
		}
		lineString := strings.TrimSpace(string(line))
		if strings.HasPrefix(lineString, "--") {
			continue
		}
		if statement.Len() > 0 {
			statement.WriteRune(' ')
		}
		statement.WriteString(lineString)
		if strings.HasSuffix(lineString, ";") {
			return statement.String(), nil
		}
	}
}
