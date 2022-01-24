// Package multistmt provides methods for parsing multi-statement database migrations
package multistmt

import (
	"bytes"
	"fmt"
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
		if i := bytes.Index(d, delimiter); i >= 0 {
			return i + len(delimiter), d[:i+len(delimiter)], nil
		}
		return 0, nil, nil
	}
}

// Parse parses the given multi-statement migration
func Parse(reader io.Reader, delimiter []byte, maxMigrationSize int, h Handler) error {
	var err error = nil
	// buf is the bytes read from input reader
	buf := make([]byte, 4096)
	// true when we're ignoring input(during comments)
	discard := false
	// accumulate statements intermediate buffer, this buffer will be incomplete
	// until end-of-statement char ';'
	accum := make([]byte, 0, 1024)
	// completed statements, contents of accum will be dumped in here
	stmts := make([][]byte, 0, 2048)
	for err == nil {
		n, err := reader.Read(buf)
		if n > 0 {
			for i := range buf[:n] {
				if len(buf) > 2 &&  buf[i] == '-' && buf[i+1] == '-' && buf[i+2] == '-' {
					discard = true
				}
				fmt.Printf("%c", buf[i])
				switch ch := buf[i]; ch {
				case ';':
					if !discard {
						// include ';' in accum
						accum = append(accum, ch)
						stmts = append(stmts, accum)
						// reset accum, maintain allocated memory
						accum = accum[:0]
					}
				case '\n':
					// at end of line, reset discard
					discard = false
				default:
					if !discard {
						accum = append(accum, ch)
					}
				}
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
	}

	for _, stmt := range stmts {
		h(stmt)
	}
	return nil
}
