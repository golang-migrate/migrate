// Package multistmt provides methods for parsing multi-statement database migrations
package multistmt

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
)

// StartBufSize is the default starting size of the buffer used to scan and parse multi-statement migrations
var StartBufSize = 4096

// Handler handles a single migration parsed from a multi-statement migration.
// It's given the single migration to handle and returns whether or not further statements
// from the multi-statement migration should be parsed and handled.
type Handler func(migration []byte) error

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
func Parse(reader io.Reader, delimiter []byte, maxMigrationSize int, replacementStatement string, h Handler) error {
	// notes:
	// 1. comment chars will be detected anywhere, a '--' in the middle of a
	//    line will start comment mode(good and bad)
	// 2. input can be arbitrarily large, but the internal buffers will be
	//    problems(like statements)
	// 3. could be converted to work with logger, for now fmt is still used
	// 4. doesn't support /* */ c-style comments (future)
	// 5. doesn't support nested comments (future)
	// 6. now supports plpgsql trigger bodies
	var err error = nil
	// buf is the bytes read from input reader
	buf := make([]byte, 1024)
	// true when we're ignoring input(during comments)
	discard := false
	// fnbody is true when a function body delimiters $$ are encountered
	fnbody := false
	// accumulate statements intermediate buffer, this buffer will be incomplete
	// until end-of-statement char ';'
	accum := make([]byte, 0, 2048)
	// completed statements, contents of accum will be dumped in here
	stmts := make([][]byte, 0, 1000)

	for err == nil {
		n, err := reader.Read(buf)
		if n > 0 {
			for i := range buf[:n] {
				if !fnbody {
					// when first two chars are comment indicators.
					switch {
					// ignore all lines that start with --
					case len(buf) > 1 && i+1 < len(buf) && buf[i] == '-' && buf[i+1] == '-':
						discard = true
					// ignore any lines that start with // (this also covers ///)
					case len(buf) > 1 && i+1 < len(buf) && buf[i] == '/' && buf[i+1] == '/':
						discard = true
					}
				}
				// output the content, for logging
				fmt.Printf("%c", buf[i])
				switch ch := buf[i]; ch {
				case '$':
					// look around is there another $?
					// is there also and ending marker like "$$ LANGUAGE plpgsql"
					if len(buf) >= i+1 && buf[i+1] == '$' {
						// set fnbody false to trigger the check for the next `;`
						fnbody = !fnbody
					}
					if !discard {
						accum = append(accum, ch)
					}
				case ';':
					if fnbody {
						accum = append(accum, ch)
						continue
					}
					if !discard {
						// include ';' in accum
						accum = append(accum, ch)
						c1 := make([]byte, len(accum))
						copy(c1, accum)
						if replacementStatement != "" {
							s1 := strings.ReplaceAll(string(c1), "<SCHEMA_NAME>", replacementStatement)
							c1 = []byte(s1)
						}
						stmts = append(stmts, c1)
						// reset accum, maintain allocated memory
						accum = accum[:0]
					}
				case '\n':
					// at end of line, reset discard
					discard = false
					if fnbody {
						accum = append(accum, ch)
					}
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

	for i, stmt := range stmts {
		fmt.Println(i, string(stmt))
		if err := h(stmt); err != nil {
			return errors.Wrapf(err, "%s", stmt)
		}
	}
	return nil
}
