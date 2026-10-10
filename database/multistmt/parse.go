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
		// SplitFunc inspired by bufio.ScanLines() implementation.
		// A delimiter is only a statement boundary when it is NOT inside a
		// Cypher comment (`// ... ` to end of line, or `/* ... */`). Without
		// this, a `;` inside a comment (prose or example Cypher) would split a
		// comment-only chunk that the server then rejects as an empty query.
		if atEOF {
			if len(d) == 0 {
				return 0, nil, nil
			}
			return len(d), d, nil
		}
		for i := 0; i < len(d); {
			switch {
			case i+1 < len(d) && d[i] == '/' && d[i+1] == '/':
				// line comment: skip to the next newline (or need more data)
				j := bytes.IndexByte(d[i:], '\n')
				if j < 0 {
					return 0, nil, nil
				}
				i += j + 1
			case i+1 < len(d) && d[i] == '/' && d[i+1] == '*':
				// block comment: skip past the closing */ (or need more data)
				j := bytes.Index(d[i:], []byte("*/"))
				if j < 0 {
					return 0, nil, nil
				}
				i += j + 2
			case d[i] == delimiter[0]:
				return i + len(delimiter), d[:i+len(delimiter)], nil
			default:
				i++
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
