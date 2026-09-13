package elasticsearch

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// requestDelimiter separates consecutive requests within a migration file.
// It is only treated as a delimiter when it is alone on its own line, so that
// a "---" sequence inside a request body is left untouched.
const requestDelimiter = "---"

var (
	// StartBufSize is the starting size of the buffer used to scan migrations.
	StartBufSize = 4096

	// MaxMigrationSize is the maximum size of a single migration file.
	MaxMigrationSize = 10 * 1 << 20

	// allowedMethods disallows requests with "custom" methods since those
	// would probably be typos anyways. Even "TRACE", "CONNECT", "OPTIONS",
	// "HEAD", and "QUERY" could be dropped as not surfacing in the
	// Elasticsearch REST API, but they are kept here for future-proofing.
	allowedMethods = map[string]bool{
		http.MethodGet:     true,
		http.MethodPost:    true,
		http.MethodPut:     true,
		http.MethodPatch:   true,
		http.MethodDelete:  true,
		http.MethodOptions: true,
		http.MethodHead:    true,
		http.MethodConnect: true,
		http.MethodTrace:   true,
		// Not in net/http yet, but RFC 10008 has been accepted.
		"QUERY": true,
	}
)

// request is a single request parsed out of a migration file, ready to be sent
// once its path has been resolved against the cluster url.
type request struct {
	*http.Request

	// Line is the 1-indexed line of the request line within the migration
	// file. It is reported back to the user when a request fails.
	Line uint
}

func (r request) String() string {
	return r.Method + " " + r.URL.String()
}

// parseRequests parses a migration file into the requests it contains.
//
// A file is a list of requests separated by a line containing only "---".
// Each request is written in plain HTTP:
//
//	POST /index/_doc HTTP/1.1
//	Content-Type: application/json
//
//	{"field": "value"}
//
// The HTTP version is optional and ignored. Blank chunks are skipped, so a
// file may start with a delimiter. Lines starting with "#" or "//" before the
// request line are comments.
func parseRequests(r io.Reader) ([]request, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, StartBufSize), MaxMigrationSize)

	var (
		requests []request
		chunk    []string
		lineNo   uint
	)
	chunkStart := uint(1)

	flush := func() error {
		req, ok, err := parseRequest(chunk, chunkStart)
		if err != nil {
			return err
		}
		if ok {
			requests = append(requests, req)
		}
		return nil
	}

	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if strings.TrimSpace(line) == requestDelimiter {
			if err := flush(); err != nil {
				return nil, err
			}
			chunk = chunk[:0]
			chunkStart = lineNo + 1
			continue
		}
		chunk = append(chunk, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}

	return requests, nil
}

// parseRequest parses a single chunk of a migration file. It reports whether
// the chunk held a request at all, so that empty chunks can be skipped.
func parseRequest(lines []string, chunkStart uint) (request, bool, error) {
	i := 0
	for ; i < len(lines); i++ {
		if trimmed := strings.TrimSpace(lines[i]); trimmed != "" && !isComment(trimmed) {
			break
		}
	}
	if i == len(lines) {
		return request{}, false, nil
	}

	line := chunkStart + uint(i)
	method, rawPath, err := parseRequestLine(lines[i], line)
	if err != nil {
		return request{}, false, err
	}
	i++

	header := http.Header{}
	for ; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "" {
			i++
			break
		}
		name, value, found := strings.Cut(lines[i], ":")
		if !found {
			return request{}, false, fmt.Errorf("line %d: malformed header %q, expected \"Name: value\"",
				chunkStart+uint(i), lines[i])
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return request{}, false, fmt.Errorf("line %d: header is missing a name", chunkStart+uint(i))
		}
		header.Add(name, strings.TrimSpace(value))
	}

	// The body keeps a trailing newline: the bulk and msearch apis reject a
	// request whose last line is not terminated. A strings.Reader gives the
	// request a known length and a GetBody, so it needs no chunked encoding.
	var body io.Reader
	if b := strings.TrimSpace(strings.Join(lines[i:], "\n")); b != "" {
		body = strings.NewReader(b + "\n")
	}

	req, err := http.NewRequest(method, rawPath, body)
	if err != nil {
		return request{}, false, fmt.Errorf("line %d: %w", line, err)
	}
	req.Header = header

	return request{Request: req, Line: line}, true, nil
}

func parseRequestLine(raw string, line uint) (method, rawPath string, err error) {
	fields := strings.Fields(raw)
	switch {
	case len(fields) < 2:
		return "", "", fmt.Errorf("line %d: malformed request line %q, expected \"METHOD /path\"", line, raw)
	case len(fields) > 3:
		return "", "", fmt.Errorf("line %d: malformed request line %q, expected \"METHOD /path [HTTP/1.1]\"", line, raw)
	case len(fields) == 3 && !strings.HasPrefix(fields[2], "HTTP/"):
		return "", "", fmt.Errorf("line %d: expected an HTTP version after the path, got %q", line, fields[2])
	}

	if !allowedMethods[fields[0]] {
		return "", "", fmt.Errorf("line %d: unsupported HTTP method %q", line, fields[0])
	}
	if !strings.HasPrefix(fields[1], "/") {
		return "", "", fmt.Errorf("line %d: request path %q must start with \"/\"", line, fields[1])
	}

	return fields[0], fields[1], nil
}

func isComment(line string) bool {
	return strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//")
}
