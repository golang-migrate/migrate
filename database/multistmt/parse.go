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

		inLineComment := false
		inBlockComment := false
		inSingleQuote := false
		inDoubleQuote := false

		delLen := len(delimiter)
		for i := 0; i < len(d); i++ {
			c := d[i]

			// State machine to ignore delimiters inside comments and strings
			if inLineComment {
				if c == '\n' {
					inLineComment = false
				}
				continue
			}

			if inBlockComment {
				if c == '*' && i+1 < len(d) && d[i+1] == '/' {
					inBlockComment = false
					i++
				}
				continue
			}

			if inSingleQuote {
				if c == '\'' {
					if i+1 < len(d) && d[i+1] == '\'' {
						i++ // escaped single quote ''
					} else {
						inSingleQuote = false
					}
				}
				continue
			}

			if inDoubleQuote {
				if c == '"' {
					if i+1 < len(d) && d[i+1] == '"' {
						i++ // escaped double quote ""
					} else {
						inDoubleQuote = false
					}
				}
				continue
			}

			// Check for comments start
			if c == '-' && i+1 < len(d) && d[i+1] == '-' {
				inLineComment = true
				i++
				continue
			}
			if c == '/' && i+1 < len(d) && d[i+1] == '*' {
				inBlockComment = true
				i++
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

			// Check for delimiter match outside comments/strings
			if i+delLen <= len(d) && bytes.Equal(d[i:i+delLen], delimiter) {
				return i + delLen, d[:i+delLen], nil
			}
		}

		return 0, nil, nil
	}
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
