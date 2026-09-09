// Package multistmt provides methods for parsing multi-statement database migrations
package multistmt

import (
	"bufio"
	"bytes"
	"io"
)

// StartBufSize is the default starting size of the buffer used to scan and parse multi-statement migrations
var StartBufSize = 4096

// Handler handles a single migration parsed from a multi-statement migration.
// It's given the single migration to handle and returns whether or not further statements
// from the multi-statement migration should be parsed and handled.
type Handler func(migration []byte) bool

func splitWithDelimiter(delimiter []byte) func(d []byte, atEOF bool) (int, []byte, error) {
	return func(d []byte, atEOF bool) (int, []byte, error) {
		// SplitFunc inspired by bufio.ScanLines() implementation
		if atEOF {
			if len(d) == 0 {
				return 0, nil, nil
			}
			return len(d), d, nil
		}
		if i := findDelimiterOutsideQuotes(d, delimiter); i >= 0 {
			return i + len(delimiter), d[:i+len(delimiter)], nil
		}
		return 0, nil, nil
	}
}

// findDelimiterOutsideQuotes finds the first occurrence of delimiter in d
// that is not inside a single-quoted or double-quoted string.
// Returns -1 if not found.
func findDelimiterOutsideQuotes(d []byte, delimiter []byte) int {
	inSingleQuote := false
	inDoubleQuote := false
	delimLen := len(delimiter)

	for i := 0; i < len(d); i++ {
		c := d[i]

		if inSingleQuote {
			if c == '\'' {
				// Check for escaped quote ('')
				if i+1 < len(d) && d[i+1] == '\'' {
					i++ // skip escaped quote
				} else {
					inSingleQuote = false
				}
			}
			continue
		}

		if inDoubleQuote {
			if c == '"' {
				if i+1 < len(d) && d[i+1] == '"' {
					i++ // skip escaped quote
				} else {
					inDoubleQuote = false
				}
			}
			continue
		}

		if c == '\'' {
			inSingleQuote = true
			continue
		}
		if c == '"' {
			inDoubleQuote = true
			continue
		}

		// Check for delimiter match
		if i+delimLen <= len(d) && bytes.Equal(d[i:i+delimLen], delimiter) {
			return i
		}
	}
	return -1
}

// Parse parses the given multi-statement migration
func Parse(reader io.Reader, delimiter []byte, maxMigrationSize int, h Handler) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, StartBufSize), maxMigrationSize)
	scanner.Split(splitWithDelimiter(delimiter))
	for scanner.Scan() {
		cont := h(scanner.Bytes())
		if !cont {
			break
		}
	}
	return scanner.Err()
}
