package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
)

const (
	alreadyExistsErrorType = "resource_already_exists_exception"

	// deleteURIBudget caps the length of the path of a single delete request.
	// Elasticsearch rejects a request line longer than http.max_initial_line_length,
	// 4kb by default, so names are batched to stay under it with room to spare
	// for the query string and the "DELETE ... HTTP/1.1" framing.
	deleteURIBudget = 3000

	errListIndices     = "failed to list indices"
	errListDataStreams = "failed to list data streams"
)

// client is a thin wrapper around the Elasticsearch REST API.
// It's more generic than adding elasticsearch client libraries as dependencies,
// and it leaves it to the user to send the appropriate requests for their cluster version.
// It's also more lightweight than packaging a full on es client.
type client struct {
	httpClient *http.Client
	baseURL    *url.URL
	user       *url.Userinfo
}

// newClient returns a client talking to clusterURL. Only the scheme, host and
// path prefix of clusterURL are kept: the credentials are carried separately so
// that they are never rendered into a request uri, and the query string holds
// driver configuration only.
func newClient(httpClient *http.Client, clusterURL *url.URL) *client {
	base := *clusterURL
	base.User = nil
	base.RawQuery = ""
	base.Fragment = ""
	base.Path = strings.TrimSuffix(base.Path, "/")

	return &client{
		httpClient: httpClient,
		baseURL:    &base,
		user:       clusterURL.User,
	}
}

func (c *client) close() {
	c.httpClient.CloseIdleConnections()
}

// do builds a request against the cluster and sends it.
func (c *client) do(method, rawPath string, body []byte) (*response, error) {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, rawPath, reader)
	if err != nil {
		return nil, err
	}
	return c.send(req)
}

// send resolves a request built against a bare path, such as one parsed out of
// a migration, into one aimed at the cluster, then sends it. The request passed
// in is left untouched, so that a caller reporting on a failed request still
// sees the path as it was written.
func (c *client) send(req *http.Request) (*response, error) {
	u, err := c.resolve(req.URL)
	if err != nil {
		return nil, err
	}

	sendable := req.Clone(req.Context())
	sendable.URL = u
	if sendable.Body != nil && sendable.Header.Get("Content-Type") == "" {
		sendable.Header.Set("Content-Type", "application/json")
	}
	if c.user != nil {
		password, _ := c.user.Password()
		sendable.SetBasicAuth(c.user.Username(), password)
	}

	resp, err := c.httpClient.Do(sendable)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	read, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &response{StatusCode: resp.StatusCode, Body: read}, nil
}

// resolve turns a request path, query string included, into an absolute url
// under the cluster url.
func (c *client) resolve(ref *url.URL) (*url.URL, error) {
	if ref.IsAbs() || ref.Host != "" {
		return nil, fmt.Errorf("request path %q must be relative to the cluster url", ref)
	}

	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, ref.Path)
	if !isUnder(u.Path, c.baseURL.Path) {
		return nil, fmt.Errorf("request path %q escapes the cluster url path %q", ref, c.baseURL.Path)
	}
	// The path is also carried in its original encoding, so that characters Go
	// would otherwise escape reach the cluster as written. Index patterns such
	// as "*" are legal in a uri path and Elasticsearch expects them unescaped.
	u.RawPath = path.Join(c.baseURL.EscapedPath(), ref.EscapedPath())
	u.RawQuery = ref.RawQuery
	return &u, nil
}

// isUnder reports whether p is base itself or sits below it.
// p is cleaned before the prefix check so callers don't need to pre-clean it.
func isUnder(p, base string) bool {
	base = strings.TrimSuffix(base, "/")
	if base == "" {
		return true
	}
	p = path.Clean("/" + strings.TrimPrefix(p, "/"))
	return p == base || strings.HasPrefix(p, base+"/")
}

func (c *client) ping() error {
	resp, err := c.do(http.MethodGet, "/", nil)
	if err != nil {
		return err
	}
	if !resp.ok() {
		return resp.err()
	}
	return nil
}

// createIndex creates an index, treating an index that already exists as
// success.
func (c *client) createIndex(name string, body []byte) error {
	resp, err := c.do(http.MethodPut, "/"+name, body)
	if err != nil {
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to create index %s", name)}
	}
	if resp.ok() || resp.errorType() == alreadyExistsErrorType {
		return nil
	}
	return &database.Error{OrigErr: resp.err(), Err: fmt.Sprintf("failed to create index %s", name)}
}

// indices resolves an index pattern to the concrete index names it matches,
// leaving out the system indices Elasticsearch manages itself.
func (c *client) indices(pattern string) ([]string, error) {
	p := fmt.Sprintf("/_cat/indices/%s?format=json&h=index&expand_wildcards=open,closed", pattern)
	resp, err := c.do(http.MethodGet, p, nil)
	if err != nil {
		return nil, &database.Error{OrigErr: err, Err: errListIndices}
	}
	// Nothing matches the pattern, so there is nothing to report.
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if !resp.ok() {
		return nil, &database.Error{OrigErr: resp.err(), Err: errListIndices}
	}

	var entries []struct {
		Index string `json:"index"`
	}
	if err := json.Unmarshal(resp.Body, &entries); err != nil {
		return nil, &database.Error{OrigErr: err, Err: errListIndices}
	}

	indices := make([]string, 0, len(entries))
	for _, e := range entries {
		// System indices, such as .geoip_databases, are named with a leading
		// dot and are reserved: deleting them is rejected by the cluster.
		if strings.HasPrefix(e.Index, ".") {
			continue
		}
		indices = append(indices, e.Index)
	}
	return indices, nil
}

// dataStreams resolves an index pattern to the data streams it matches, leaving
// out the system ones Elasticsearch manages itself.
func (c *client) dataStreams(pattern string) ([]string, error) {
	resp, err := c.do(http.MethodGet, "/_data_stream/"+pattern, nil)
	if err != nil {
		return nil, &database.Error{OrigErr: err, Err: errListDataStreams}
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		// Nothing matches the pattern, so there is nothing to report.
		return nil, nil
	case resp.StatusCode == http.StatusMethodNotAllowed, resp.StatusCode == http.StatusBadRequest:
		// The cluster predates data streams, so there are none to report.
		return nil, nil
	case !resp.ok():
		return nil, &database.Error{OrigErr: resp.err(), Err: errListDataStreams}
	}

	var body struct {
		DataStreams []struct {
			Name string `json:"name"`
		} `json:"data_streams"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return nil, &database.Error{OrigErr: err, Err: errListDataStreams}
	}

	names := make([]string, 0, len(body.DataStreams))
	for _, stream := range body.DataStreams {
		// System data streams, such as .fleet-actions-results, are named with a
		// leading dot and are reserved, exactly like system indices.
		if strings.HasPrefix(stream.Name, ".") {
			continue
		}
		names = append(names, stream.Name)
	}
	return names, nil
}

// deleteBatched deletes named resources under a path, a batch at a time. A name
// that is already gone is not an error: the point is that it no longer exists.
func (c *client) deleteBatched(prefix string, names []string, failure string) error {
	for start := 0; start < len(names); {
		// Take as many names as fit in the uri budget, but always at least one,
		// so that a single name longer than the budget is still attempted rather
		// than looping forever.
		end, size := start, len(prefix)
		for end < len(names) && (end == start || size+len(",")+len(names[end]) <= deleteURIBudget) {
			if end > start {
				size += len(",")
			}
			size += len(names[end])
			end++
		}

		resp, err := c.do(http.MethodDelete, prefix+strings.Join(names[start:end], ","), nil)
		if err != nil {
			return &database.Error{OrigErr: err, Err: failure}
		}
		if !resp.ok() && resp.StatusCode != http.StatusNotFound {
			return &database.Error{OrigErr: resp.err(), Err: failure}
		}
		start = end
	}
	return nil
}

// response is a fully read response from the cluster. Bodies are small enough
// to buffer, and doing so keeps connection handling in one place.
type response struct {
	StatusCode int
	Body       []byte
}

func (r *response) ok() bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

// errorType returns the Elasticsearch error type of a failed response, such as
// "resource_already_exists_exception", or an empty string when the response
// carries no structured error.
func (r *response) errorType() string {
	var body struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.Body, &body); err != nil {
		return ""
	}
	return body.Error.Type
}

// err builds an error out of a failed response, preferring the reason reported
// by the cluster over the raw body.
func (r *response) err() error {
	var body struct {
		Error struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.Body, &body); err == nil && body.Error.Type != "" {
		return fmt.Errorf("%s: %s (status %d)", body.Error.Type, body.Error.Reason, r.StatusCode)
	}
	return fmt.Errorf("unexpected status %d: %s", r.StatusCode, bytes.TrimSpace(r.Body))
}
