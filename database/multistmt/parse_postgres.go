// Package multistmt provides methods for parsing multi-statement database migrations
package multistmt

import (
	"bufio"
	"bytes"
	"io"
	"unicode"

	"golang.org/x/exp/slices"
)

const dollar = '$'

func isValidTagSymbol(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func pgSplitWithDelimiter(delimiter []byte) bufio.SplitFunc {
	// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING
	// inside the dollar-quoted string, single quotes can be used without needing to
	// be escaped. Indeed, no characters inside a dollar-quoted string are ever
	// escaped: the string content is always written literally. Backslashes are not
	// special, and neither are dollar signs, unless they are part of a sequence
	// matching the opening tag.
	//
	// It is possible to nest dollar-quoted string constants by choosing different
	// tags at each nesting level. This is most commonly used in writing function
	// definitions
	return func(d []byte, atEOF bool) (int, []byte, error) {
		if atEOF {
			if len(d) == 0 {
				return 0, nil, nil
			}

			return len(d), d, nil
		}

		stack := [][]byte{delimiter}
		maybeDollarQuoted := false
		firstDollarPosition := 0

		reader := bufio.NewReader(bytes.NewReader(d))
		position := 0

		for position < len(d) {
			currentDelimiter := stack[len(stack)-1]

			if len(d[position:]) >= len(currentDelimiter) {
				if slices.Equal(d[position:position+len(currentDelimiter)], currentDelimiter) {
					// pop delimiter from stack and fast-forward cursor and reader
					stack = stack[:len(stack)-1]
					position += len(currentDelimiter)
					_, _ = io.ReadFull(reader, currentDelimiter)

					if len(stack) != 0 {
						continue
					}
				}
			}

			if len(stack) == 0 {
				return position, d[:position], nil
			}

			r, size, err := reader.ReadRune()
			if err != nil {
				return position + size, d[:position+size], err
			}

			switch {
			case r == dollar && !maybeDollarQuoted:
				maybeDollarQuoted = true

				firstDollarPosition = position
			case r == dollar && maybeDollarQuoted:
				stack = append(stack, d[firstDollarPosition:position+size])
				maybeDollarQuoted = false
			case !isValidTagSymbol(r) && maybeDollarQuoted:
				maybeDollarQuoted = false
			}

			position += size
		}

		return 0, nil, nil
	}
}

// PGParse parses the given multi-statement migration for PostgreSQL respecting the dollar-quoted strings
func PGParse(reader io.Reader, delimiter []byte, maxMigrationSize int, h Handler) error {
	return parse(reader, delimiter, maxMigrationSize, h, pgSplitWithDelimiter)
}
