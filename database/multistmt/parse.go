// Package multistmt provides methods for parsing multi-statement database migrations
package multistmt

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
)

// ParseBufSize is the buffer size for the multi-statement reader
var ParseBufSize = 1024

// ParseTrace is a flag that enables tracing during parsing
var ParseTrace bool

// Handler handles a single migration parsed from a multi-statement migration.
// It's given the single migration to handle and returns whether or not further statements
// from the multi-statement migration should be parsed and handled.
type Handler func(migration []byte) error

// Parse parses the given multi-statement migration
func Parse(reader io.Reader, _ []byte, _ int, replacementStatement string, h Handler) error {
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
	buf := make([]byte, ParseBufSize)
	// true when we're ignoring input(during comments)
	discard := false
	// fnbody is true when a function body delimiters $$ are encountered
	fnbody := false
	// accumulate statements intermediate buffer, this buffer will be incomplete
	// until end-of-statement char ';'
	accum := make([]byte, 0, 2048)
	// tmp is a carry-over buffer used when doing look ahead,
	// tmp is the characters in the buf that are insufficient to do a look ahead
	// comparison but could be combined with characters further in the stream.
	var tmp []byte
	// counter is using during tracing to keep track of a total number of characters
	counter := 0
	for err == nil {
		n, err := reader.Read(buf)
		trace("tmp(2): '%s', buf: %s, discard: %v\n", tmp, buf, discard)
		// tmp is the carry-over buffer,
		// if the previous loop iteration had two few characters to make comparisions,
		// tmp will have the characters at the point the loop iteration was abandoned(
		// break'd out of)
		trace("prepending '%s' to buf: %s\n", tmp, buf)
		buf = append(tmp, buf...)
		trace("len(buf): %d, buf: %s\n", len(buf), buf)
		// n needs to include the length of tmp since it was copied into n,
		// but n originally only held the number of chars read from the
		// reader.Read(buf) call.
		n = n + len(tmp)
		// erase tmp, we put tmp's contents in buf
		tmp = tmp[:0]

		if n > 0 {
			// buf needs capacity(it is initialized with capapcity and length the same)
			// so we can only loop to the bytes read, not the capacity nor length
			// there may also be bytes copied over from the previous loop interation
			// that are now in buf also.
			for i := range buf[:n] {
				// 2 here is the number of look ahead characters that we use.
				// This tmp buffer is used to copy over bytes from the current loop
				// iteration if there are not enough characters to lookahead and find a match
				if i+1 >= len(buf) {
					tmp = make([]byte, n-i)
					trace("copying '%s' to tmp %s, len(tmp): %d\n", buf[i:n],
						tmp,
						len(tmp))

					copy(tmp, buf[i:n])
					trace("carry bytes over i: %v, n: %v, len(buf): %v, "+
						"%s\n", i, n,
						len(buf),
						string(tmp))
					break
				}
				if !fnbody {
					// when first two chars are comment indicators.
					switch {
					// ignore all lines that start with --
					case len(buf) > 1 && i+1 < len(buf) && buf[i] == '-' && buf[i+1] == '-':
						trace("comment\n")
						discard = true
					// ignore any lines that start with // (this also covers ///)
					case len(buf) > 1 && i+1 < len(buf) && buf[i] == '/' && buf[i+1] == '/':
						discard = true
					}
				}
				// output the content, for logging
				if buf[i] == ' ' {
					trace("%d.\n", counter+i)
				} else if buf[i] == '\t' {
					trace("%d\\t\n", counter+i)
				} else {
					trace("%d '%c'\n", counter+i, buf[i])
				}
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
					trace("discard(1): %v, fnbody: %v, i: %v, len(buf): %v\n",
						discard, fnbody,
						i, len(buf))
					if fnbody {
						accum = append(accum, ch)
						continue
					}
					if !discard {
						// include ';' in accum
						accum = append(accum, ch)
						stmt := make([]byte, len(accum))
						copy(stmt, accum)
						if replacementStatement != "" {
							stmt = bytes.ReplaceAll(stmt, []byte("<SCHEMA_NAME>"),
								[]byte(replacementStatement))
						}

						// fully formed statement(stmt), exec the statement
						trace("%s\n", string(stmt))
						if err := h(stmt); err != nil {
							return errors.Wrapf(err, "%s", stmt)
						}
						// reset accum, maintain allocated memory
						accum = accum[:0]
					}
				case '\n':
					// at end of line, reset discard
					discard = false
					if fnbody {
						accum = append(accum, ch)
					}
					trace("discard(2): %v, fnbody: %v, i: %v, len(buf): %v\n",
						discard, fnbody,
						i, len(buf))
				default:
					if !discard {
						accum = append(accum, ch)
					}
				}
			}
			trace("tmp(1): '%s'\n", tmp)
		}
		// keep a counter of the characters we've seen, used for debugging/tracing output
		counter = counter + n - len(tmp)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// trace output tracing when tracing enabled by the ParseTrace variable
func trace(spec string, args ...interface{}) {
	if !ParseTrace {
		return
	}
	fmt.Printf(spec, args...)
}
