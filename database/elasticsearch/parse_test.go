package elasticsearch

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const testPath = "/books"

// expectedRequest describes a parsed request in the terms the migration file
// is written in.
type expectedRequest struct {
	method string
	path   string
	header http.Header
	body   string
	line   uint
}

// readBody reads a parsed request's body, which is nil when the request has
// none. The content length must match what was read, since it is what the
// cluster is told to expect.
func readBody(t *testing.T, r request) string {
	t.Helper()

	if r.Body == nil {
		if r.ContentLength != 0 {
			t.Errorf("got content length %d for a request with no body", r.ContentLength)
		}
		return ""
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(body)) != r.ContentLength {
		t.Errorf("got content length %d, expected %d", r.ContentLength, len(body))
	}
	return string(body)
}

func TestParseRequests(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []expectedRequest
	}{
		{
			name:     "empty migration",
			input:    "",
			expected: nil,
		},
		{
			name:     "delimiters only",
			input:    "---\n---\n",
			expected: nil,
		},
		{
			name:  "leading delimiter",
			input: "---\nPUT /simple_index HTTP/1.1\nContent-Type: application/json\n\n{\"k\": \"v\"}\n",
			expected: []expectedRequest{{
				method: http.MethodPut,
				path:   "/simple_index",
				header: http.Header{"Content-Type": []string{"application/json"}},
				body:   "{\"k\": \"v\"}\n",
				line:   2,
			}},
		},
		{
			name:  "no delimiter, no headers, no body",
			input: "DELETE /simple_index\n",
			expected: []expectedRequest{{
				method: http.MethodDelete,
				path:   "/simple_index",
				header: http.Header{},
				line:   1,
			}},
		},
		{
			name:  "query string is preserved",
			input: "POST /books/_doc/1?refresh=true&op_type=create\n",
			expected: []expectedRequest{{
				method: http.MethodPost,
				path:   "/books/_doc/1?refresh=true&op_type=create",
				header: http.Header{},
				line:   1,
			}},
		},
		{
			name:  "comments are skipped and do not shift the line number",
			input: "# create the index\n// and nothing else\nPUT /books\n",
			expected: []expectedRequest{{
				method: http.MethodPut,
				path:   testPath,
				header: http.Header{},
				line:   3,
			}},
		},
		{
			name:  "several requests",
			input: "PUT /books\n\n{\"a\": 1}\n---\nDELETE /books\n---\n\nHEAD /books\n",
			expected: []expectedRequest{
				{method: http.MethodPut, path: testPath, header: http.Header{}, body: "{\"a\": 1}\n", line: 1},
				{method: http.MethodDelete, path: testPath, header: http.Header{}, line: 5},
				{method: http.MethodHead, path: testPath, header: http.Header{}, line: 8},
			},
		},
		{
			name:  "delimiter inside a body is not a delimiter",
			input: "POST /books/_doc\nContent-Type: application/json\n\n{\"title\": \"---\"}\n",
			expected: []expectedRequest{{
				method: http.MethodPost,
				path:   "/books/_doc",
				header: http.Header{"Content-Type": []string{"application/json"}},
				body:   "{\"title\": \"---\"}\n",
				line:   1,
			}},
		},
		{
			name:  "multi line body",
			input: "POST /books/_doc\n\n{\n  \"a\": 1\n}\n",
			expected: []expectedRequest{{
				method: http.MethodPost,
				path:   "/books/_doc",
				header: http.Header{},
				body:   "{\n  \"a\": 1\n}\n",
				line:   1,
			}},
		},
		{
			name: "ndjson body keeps its terminator",
			input: "POST /_bulk\nContent-Type: application/x-ndjson\n\n" +
				"{\"index\":{\"_index\":\"books\"}}\n{\"title\":\"A Wizard of Earthsea\"}\n",
			expected: []expectedRequest{{
				method: http.MethodPost,
				path:   "/_bulk",
				header: http.Header{"Content-Type": []string{"application/x-ndjson"}},
				// The bulk api rejects a request whose last line is not
				// terminated, so the trailing newline must survive parsing.
				body: "{\"index\":{\"_index\":\"books\"}}\n{\"title\":\"A Wizard of Earthsea\"}\n",
				line: 1,
			}},
		},
		{
			name:  "repeated headers",
			input: "GET /books\nX-Thing: a\nX-Thing: b\n",
			expected: []expectedRequest{{
				method: http.MethodGet,
				path:   testPath,
				header: http.Header{"X-Thing": []string{"a", "b"}},
				line:   1,
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requests, err := parseRequests(strings.NewReader(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if len(requests) != len(tc.expected) {
				t.Fatalf("got %d requests, expected %d: %v", len(requests), len(tc.expected), requests)
			}
			for i, expected := range tc.expected {
				got := requests[i]
				if got.Method != expected.method {
					t.Errorf("request %d: got method %q, expected %q", i, got.Method, expected.method)
				}
				if got.URL.String() != expected.path {
					t.Errorf("request %d: got path %q, expected %q", i, got.URL, expected.path)
				}
				if got.Line != expected.line {
					t.Errorf("request %d: got line %d, expected %d", i, got.Line, expected.line)
				}
				if body := readBody(t, got); body != expected.body {
					t.Errorf("request %d: got body %q, expected %q", i, body, expected.body)
				}
				if len(got.Header) != len(expected.header) {
					t.Errorf("request %d: got headers %v, expected %v", i, got.Header, expected.header)
					continue
				}
				for name, values := range expected.header {
					if strings.Join(got.Header.Values(name), ",") != strings.Join(values, ",") {
						t.Errorf("request %d: got header %s=%v, expected %v", i, name, got.Header.Values(name), values)
					}
				}
			}
		})
	}
}

func TestParseRequestsErrors(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		// expected is a fragment of the error message, which must also point
		// at the offending line.
		expected string
		line     int
	}{
		{name: "missing path", input: "GET\n", expected: "malformed request line", line: 1},
		{name: "relative path", input: "GET books\n", expected: `must start with "/"`, line: 1},
		{name: "lowercase method", input: "get /books\n", expected: "unsupported HTTP method", line: 1},
		{name: "trailing junk", input: "GET /books HTTP/1.1 nope\n", expected: "malformed request line", line: 1},
		{name: "bad http version", input: "GET /books 1.1\n", expected: "expected an HTTP version", line: 1},
		{name: "malformed header", input: "---\n---\nGET /books\nnot a header\n", expected: "malformed header", line: 4},
		{name: "nameless header", input: "GET /books\n: value\n", expected: "header is missing a name", line: 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseRequests(strings.NewReader(tc.input))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.expected) {
				t.Errorf("got error %q, expected it to contain %q", err, tc.expected)
			}
			if expected := fmt.Sprintf("line %d", tc.line); !strings.Contains(err.Error(), expected) {
				t.Errorf("got error %q, expected it to contain %q", err, expected)
			}
		})
	}
}

func TestParseRequestsMethods(t *testing.T) {
	for _, method := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "CONNECT", "OPTIONS",
		"TRACE", "QUERY"} {
		t.Run(method, func(t *testing.T) {
			requests, err := parseRequests(strings.NewReader(method + " /books\n"))
			if err != nil {
				t.Fatal(err)
			}
			if len(requests) != 1 || requests[0].Method != method {
				t.Fatalf("got %v, expected a single %s request", requests, method)
			}
		})
	}

	for _, method := range []string{"get", "Get", "FETCH", "PUT2"} {
		t.Run("rejects "+method, func(t *testing.T) {
			if _, err := parseRequests(strings.NewReader(method + " /books\n")); err == nil {
				t.Fatalf("expected %s to be rejected", method)
			}
		})
	}
}

// TestParseRequestsTooLong checks that a migration line past the scanner's
// limit is reported rather than silently truncated.
func TestParseRequestsTooLong(t *testing.T) {
	// The starting buffer has to shrink along with the limit: the scanner only
	// complains once it needs to grow past the limit.
	previousStart, previousMax := StartBufSize, MaxMigrationSize
	StartBufSize, MaxMigrationSize = 16, 64
	defer func() { StartBufSize, MaxMigrationSize = previousStart, previousMax }()

	input := "PUT /books\n\n{\"a\": \"" + strings.Repeat("x", 200) + "\"}\n"
	if _, err := parseRequests(strings.NewReader(input)); err == nil {
		t.Fatal("expected an error, got nil")
	}
}
