package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const esPort = 9200
const httpScheme = "http"
const httpsScheme = "https"
const baseEsURL = httpScheme + "://es:9200"

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		ReadyTimeout: 5 * time.Minute,
		Env: map[string]string{
			"discovery.type": "single-node",
			// Security is off so that the tests can talk plain http to the
			// cluster. Elasticsearch 8 and later enable it by default.
			"xpack.security.enabled": "false",
			// Elasticsearch sizes its heap after the host memory, which is far
			// more than these tests need.
			"ES_JAVA_OPTS": "-Xms512m -Xmx512m",
		},
	}
	// Supported versions: https://www.elastic.co/support/eol
	specs = []dktesting.ContainerSpec{
		{ImageName: "docker.elastic.co/elasticsearch/elasticsearch:7.17.25", Options: opts},
		{ImageName: "docker.elastic.co/elasticsearch/elasticsearch:8.17.0", Options: opts},
		{ImageName: "docker.elastic.co/elasticsearch/elasticsearch:9.0.0", Options: opts},
	}

	migration = []byte(`
PUT /migration_test_index HTTP/1.1
Content-Type: application/json

{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}
---
POST /migration_test_index/_doc/1?refresh=true HTTP/1.1
Content-Type: application/json

{"title": "a document"}
`)
)

func Test(t *testing.T) {
	// Elasticsearch containers are slow to start, so every check runs against
	// the same one, each on its own indices. The two that end up dropping every
	// index of the cluster are ordered first and last.
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		t.Run("test", func(t *testing.T) { test(t, c) })
		t.Run("withInstance", func(t *testing.T) { testWithInstance(t, c) })
		t.Run("customMigrationsIndex", func(t *testing.T) { testCustomMigrationsIndex(t, c) })
		t.Run("failedMigration", func(t *testing.T) { testFailedMigration(t, c) })
		t.Run("crossInstanceLock", func(t *testing.T) { testCrossInstanceLock(t, c) })
		t.Run("bulkMigration", func(t *testing.T) { testBulkMigration(t, c) })
		t.Run("requestTimeout", func(t *testing.T) { testRequestTimeout(t, c) })
		t.Run("dropIndicesPattern", func(t *testing.T) { testDropIndicesPattern(t, c) })
		t.Run("dropDataStream", func(t *testing.T) { testDropDataStream(t, c) })
		t.Run("migrate", func(t *testing.T) { testMigrate(t, c) })
	})

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func test(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "")
	defer closeDriver(t, d)

	dt.Test(t, d, migration)
}

func testMigrate(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "")
	defer closeDriver(t, d)

	m, err := migrate.NewWithDatabaseInstance("file://./examples", "", d)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)
}

func testWithInstance(t *testing.T, c dktest.ContainerInfo) {
	clusterURL, err := url.Parse("http://" + addr(t, c) + "/")
	if err != nil {
		t.Fatal(err)
	}

	d, err := WithInstance(nil, clusterURL, &Config{
		MigrationsIndex: "with_instance_migrations",
		Locking:         Locking{IndexName: "with_instance_lock", Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeDriver(t, d)

	dt.TestNilVersion(t, d)
	dt.TestLockAndUnlock(t, d)
}

func testCustomMigrationsIndex(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "?x-migrations-index=custom_migrations&x-advisory-lock-index=custom_lock")
	defer closeDriver(t, d)

	if err := d.SetVersion(3, false); err != nil {
		t.Fatal(err)
	}
	version, dirty, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != 3 || dirty {
		t.Fatalf("got version %d, dirty %v, expected 3, false", version, dirty)
	}

	// The version must have landed in the configured index, not the default one.
	if got := rawVersion(t, c, "custom_migrations"); got != 3 {
		t.Errorf("got version %d in custom_migrations, expected 3", got)
	}
}

// rawVersion reads the version document of an index straight from the cluster,
// bypassing the driver.
func rawVersion(t *testing.T, c dktest.ContainerInfo, index string) int {
	t.Helper()

	resp, err := http.Get("http://" + addr(t, c) + "/" + index + "/_doc/" + versionDocID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d reading the version of index %s", resp.StatusCode, index)
	}

	var doc struct {
		Source versionInfo `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	return doc.Source.Version
}

func testFailedMigration(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "?x-migrations-index=failed_migrations&x-advisory-lock-index=failed_lock")
	defer closeDriver(t, d)

	// The second request updates a document of an index that does not exist,
	// so the reported line must be that of the second request.
	err := d.Run(bytes.NewReader([]byte(
		"PUT /partially_applied\n---\nPOST /missing_index/_update/1\nContent-Type: application/json\n\n{\"doc\": {}}\n")))
	if err == nil {
		t.Fatal("expected the migration to fail, got nil")
	}

	var dbErr *database.Error
	if !errors.As(err, &dbErr) {
		t.Fatalf("got a %T, expected a *database.Error: %v", err, err)
	}
	if dbErr.Line != 3 {
		t.Errorf("got line %d, expected 3: %v", dbErr.Line, err)
	}
	if got := string(dbErr.Query); got != "POST /missing_index/_update/1" {
		t.Errorf("got query %q, expected the failing request line", got)
	}

	// Elasticsearch has no transactions, so the requests that already ran are
	// left in place.
	if !indexExists(t, c, "partially_applied") {
		t.Error("expected the requests before the failing one to have been applied")
	}
}

// testRequestTimeout checks that the url reaches the http client, since a
// migration running _reindex or a snapshot restore can outlast any default.
func testRequestTimeout(t *testing.T, c dktest.ContainerInfo) {
	for _, tc := range []struct {
		param    string
		expected time.Duration
	}{
		{param: "", expected: DefaultRequestTimeout},
		{param: "&x-request-timeout=5", expected: 5 * time.Second},
		// Zero disables the timeout outright.
		{param: "&x-request-timeout=0", expected: 0},
	} {
		d := open(t, c, "?x-migrations-index=timeout_migrations&x-advisory-lock-index=timeout_lock"+tc.param)

		es, ok := d.(*Elasticsearch)
		if !ok {
			t.Fatalf("got %T, expected *Elasticsearch", d)
		}
		if es.httpClient.Timeout != tc.expected {
			t.Errorf("%q: got timeout %s, expected %s", tc.param, es.httpClient.Timeout, tc.expected)
		}
		closeDriver(t, d)
	}
}

// testBulkMigration covers the ndjson apis, which reject a request whose last
// line is not terminated by a newline.
func testBulkMigration(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "?x-migrations-index=bulk_migrations&x-advisory-lock-index=bulk_lock")
	defer closeDriver(t, d)

	err := d.Run(bytes.NewReader([]byte("POST /_bulk?refresh=true\nContent-Type: application/x-ndjson\n\n" +
		"{\"index\":{\"_index\":\"bulk_books\",\"_id\":\"1\"}}\n{\"title\":\"A Wizard of Earthsea\"}\n")))
	if err != nil {
		t.Fatal(err)
	}
	if !indexExists(t, c, "bulk_books") {
		t.Error("expected the bulk migration to have created bulk_books")
	}
}

// testDropDataStream covers what the index listing cannot see: a data stream's
// backing indices are named with a leading dot and are only deletable through
// the stream itself.
func testDropDataStream(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "?x-migrations-index=ds_migrations&x-advisory-lock-index=ds_lock"+
		"&x-drop-indices-pattern=ds_logs*")
	defer closeDriver(t, d)

	err := d.Run(bytes.NewReader([]byte(
		"PUT /_index_template/ds_logs_template\nContent-Type: application/json\n\n" +
			"{\"index_patterns\": [\"ds_logs*\"], \"data_stream\": {}}\n" +
			"---\nPUT /_data_stream/ds_logs-app\n")))
	if err != nil {
		t.Fatal(err)
	}
	if !dataStreamExists(t, c, "ds_logs-app") {
		t.Fatal("expected the migration to have created the ds_logs-app data stream")
	}

	if err := d.Drop(); err != nil {
		t.Fatal(err)
	}
	if dataStreamExists(t, c, "ds_logs-app") {
		t.Error("expected drop to have removed the ds_logs-app data stream")
	}
}

func testDropIndicesPattern(t *testing.T, c dktest.ContainerInfo) {
	d := open(t, c, "?x-migrations-index=drop_test_migrations&x-advisory-lock-index=drop_test_lock"+
		"&x-drop-indices-pattern=drop_me_*")
	defer closeDriver(t, d)

	if err := d.Run(bytes.NewReader([]byte("PUT /drop_me_a\n---\nPUT /drop_me_b\n---\nPUT /keep_me\n"))); err != nil {
		t.Fatal(err)
	}
	if err := d.Drop(); err != nil {
		t.Fatal(err)
	}

	for _, index := range []string{"drop_me_a", "drop_me_b"} {
		if indexExists(t, c, index) {
			t.Errorf("expected index %s to have been dropped", index)
		}
	}
	if !indexExists(t, c, "keep_me") {
		t.Error("expected index keep_me to have been left alone")
	}
}

// testCrossInstanceLock covers what dt.TestLockAndUnlock cannot: two drivers
// contending for the same lock document, which is the case advisory locking
// exists for.
func testCrossInstanceLock(t *testing.T, c dktest.ContainerInfo) {
	query := "?x-migrations-index=cross_lock_migrations&x-advisory-lock-index=cross_lock" +
		"&x-advisory-lock-timeout=1"
	first := open(t, c, query)
	defer closeDriver(t, first)
	second := open(t, c, query)
	defer closeDriver(t, second)

	if err := first.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := second.Lock(); !errors.Is(err, database.ErrLocked) {
		t.Fatalf("got %v, expected %v", err, database.ErrLocked)
	}
	if err := first.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := second.Lock(); err != nil {
		t.Fatalf("expected the lock to be free once the first driver released it: %v", err)
	}
	if err := second.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(esPort)
	if err != nil {
		return false
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s:%s/_cluster/health?wait_for_status=yellow&timeout=1s", ip, port), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return resp.StatusCode == http.StatusOK
}

func addr(t *testing.T, c dktest.ContainerInfo) string {
	t.Helper()

	ip, port, err := c.Port(esPort)
	if err != nil {
		t.Fatal(err)
	}
	return ip + ":" + port
}

func open(t *testing.T, c dktest.ContainerInfo, query string) database.Driver {
	t.Helper()

	es := &Elasticsearch{}
	d, err := es.Open("elasticsearch://" + addr(t, c) + "/" + query)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func closeDriver(t *testing.T, d database.Driver) {
	t.Helper()

	if err := d.Close(); err != nil {
		t.Error(err)
	}
}

func dataStreamExists(t *testing.T, c dktest.ContainerInfo, name string) bool {
	t.Helper()

	resp, err := http.Get("http://" + addr(t, c) + "/_data_stream/" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusNotFound:
		return false
	default:
		t.Fatalf("unexpected status %d checking whether data stream %s exists", resp.StatusCode, name)
		return false
	}
}

func indexExists(t *testing.T, c dktest.ContainerInfo, index string) bool {
	t.Helper()

	resp, err := http.Head("http://" + addr(t, c) + "/" + index)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusNotFound:
		return false
	default:
		t.Fatalf("unexpected status %d checking whether index %s exists", resp.StatusCode, index)
		return false
	}
}

func TestOpenErrors(t *testing.T) {
	testCases := []struct {
		name     string
		url      string
		expected string
	}{
		{name: "unknown scheme", url: "http://localhost:9200/", expected: "unexpected scheme"},
		{name: "bad locking flag", url: "elasticsearch://localhost:9200/?x-advisory-locking=maybe", expected: "maybe"},
		{name: "bad lock timeout", url: "elasticsearch://localhost:9200/?x-advisory-lock-timeout=30s", expected: "30s"},
		{name: "bad lock interval", url: "elasticsearch://localhost:9200/?x-advisory-lock-timeout-interval=10s", expected: "10s"},
		{name: "bad request timeout", url: "elasticsearch://localhost:9200/?x-request-timeout=1m", expected: "1m"},
		{name: "negative request timeout", url: "elasticsearch://localhost:9200/?x-request-timeout=-1", expected: "-1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			es := &Elasticsearch{}
			_, err := es.Open(tc.url)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.expected) {
				t.Errorf("got error %q, expected it to contain %q", err, tc.expected)
			}
		})
	}
}

func TestWithInstanceErrors(t *testing.T) {
	clusterURL, err := url.Parse("http://localhost:9200/")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := WithInstance(nil, clusterURL, nil); !errors.Is(err, ErrNilConfig) {
		t.Errorf("got %v, expected %v", err, ErrNilConfig)
	}
	if _, err := WithInstance(nil, nil, &Config{}); !errors.Is(err, ErrNoClusterURL) {
		t.Errorf("got %v, expected %v", err, ErrNoClusterURL)
	}
}

func TestResolve(t *testing.T) {
	testCases := []struct {
		name     string
		base     string
		path     string
		expected string
	}{
		{name: "root", base: baseEsURL, path: "/", expected: baseEsURL + "/"},
		{name: "index", base: baseEsURL, path: "/books", expected: baseEsURL + "/books"},
		{
			name: "query string", base: baseEsURL, path: "/books/_doc/1?refresh=true",
			expected: baseEsURL + "/books/_doc/1?refresh=true",
		},
		{
			name: "path prefix", base: "http://proxy:8080/es", path: "/books/_doc/1?refresh=true",
			expected: "http://proxy:8080/es/books/_doc/1?refresh=true",
		},
		{
			name: "path prefix with trailing slash", base: "http://proxy:8080/es/", path: "/books",
			expected: "http://proxy:8080/es/books",
		},
		{name: "wildcard", base: baseEsURL, path: "/drop_me_*", expected: baseEsURL + "/drop_me_*"},
		{
			name: "escaped path", base: baseEsURL, path: "/books/_doc/a%2Fb",
			expected: baseEsURL + "/books/_doc/a%2Fb",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base, err := url.Parse(tc.base)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := url.Parse(tc.path)
			if err != nil {
				t.Fatal(err)
			}

			got, err := newClient(nil, base).resolve(ref)
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.expected {
				t.Errorf("got %q, expected %q", got, tc.expected)
			}
		})
	}
}

func TestResolveRejectsAbsoluteURLs(t *testing.T) {
	base, err := url.Parse(baseEsURL)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := url.Parse("http://evil.example.com/books")
	if err != nil {
		t.Fatal(err)
	}

	// A migration must not be able to send requests to another host.
	if _, err := newClient(nil, base).resolve(ref); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// TestResolveRejectsTraversal checks that a request path cannot climb out of
// the path prefix the cluster url points at, which on a shared host would aim
// the request at a different service.
func TestResolveRejectsTraversal(t *testing.T) {
	testCases := []struct {
		name string
		base string
		path string
	}{
		{name: "parent of prefix", base: "http://proxy:8080/es", path: "/../kibana/api/status"},
		{name: "climbs past root", base: "http://proxy:8080/es", path: "/../../etc/passwd"},
		{name: "traversal mid path", base: "http://proxy:8080/es", path: "/books/../../kibana"},
		{name: "relative traversal", base: "http://proxy:8080/es", path: "../kibana"},
		{name: "encoded traversal", base: "http://proxy:8080/es", path: "/%2e%2e/kibana"},
		{name: "sibling prefix", base: "http://proxy:8080/es", path: "/../esoteric"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base, err := url.Parse(tc.base)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := url.Parse(tc.path)
			if err != nil {
				t.Fatal(err)
			}

			got, err := newClient(nil, base).resolve(ref)
			if err == nil {
				t.Fatalf("expected an error, got %q", got)
			}
		})
	}
}

// TestResolveAllowsTraversalWithinPrefix checks that a ".." that stays under
// the cluster url is resolved rather than rejected.
func TestResolveAllowsTraversalWithinPrefix(t *testing.T) {
	testCases := []struct {
		name     string
		base     string
		path     string
		expected string
	}{
		{
			name: "back to prefix root", base: "http://proxy:8080/es", path: "/books/..",
			expected: "http://proxy:8080/es",
		},
		{
			name: "sibling index", base: "http://proxy:8080/es", path: "/books/../films",
			expected: "http://proxy:8080/es/films",
		},
		{
			name: "no prefix", base: baseEsURL, path: "/books/../films",
			expected: baseEsURL + "/films",
		},
		{
			name: "no prefix climbing past root", base: baseEsURL, path: "/../books",
			expected: baseEsURL + "/books",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base, err := url.Parse(tc.base)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := url.Parse(tc.path)
			if err != nil {
				t.Fatal(err)
			}

			got, err := newClient(nil, base).resolve(ref)
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.expected {
				t.Errorf("got %q, expected %q", got, tc.expected)
			}
		})
	}
}

// TestOpenSchemes checks that every registered scheme is rewritten to the
// transport it stands for, rather than reaching the http client as written.
func TestOpenSchemes(t *testing.T) {
	testCases := []struct {
		scheme   string
		expected string
	}{
		{scheme: "elasticsearch", expected: httpScheme},
		{scheme: "elasticsearch+http", expected: httpScheme},
		{scheme: "elasticsearchs", expected: httpsScheme},
		{scheme: "elasticsearch+https", expected: httpsScheme},
	}

	for _, tc := range testCases {
		t.Run(tc.scheme, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()

			// Only the rewrite is under test, so https is checked against the
			// url the driver built instead of a tls handshake.
			if tc.expected == httpsScheme {
				clusterURL, err := url.Parse(tc.scheme + "://es:9200/")
				if err != nil {
					t.Fatal(err)
				}
				es := &Elasticsearch{}
				if _, err := es.Open(clusterURL.String()); err == nil {
					t.Fatal("expected a connection error, got nil")
				} else if !strings.Contains(err.Error(), "https://es:9200") {
					t.Errorf("got error %q, expected it to name an https url", err)
				}
				return
			}

			rawURL := tc.scheme + "://" + srv.Listener.Addr().String() + "/?x-advisory-locking=false"
			es := &Elasticsearch{}
			d, err := es.Open(rawURL)
			if err != nil {
				t.Fatalf("Open(%q): %v", rawURL, err)
			}
			closeDriver(t, d)
		})
	}
}

// TestDeleteBatched checks that names are split into requests short enough for
// the cluster to accept, and that every name is sent exactly once.
func TestDeleteBatched(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	clusterURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Index names run to 255 characters, so a batch capped by count alone
	// overruns http.max_initial_line_length, 4kb by default.
	names := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		names = append(names, fmt.Sprintf("%s-%03d", strings.Repeat("n", 60), i))
	}

	if err := newClient(srv.Client(), clusterURL).deleteBatched("/", names, "drop failed"); err != nil {
		t.Fatal(err)
	}
	if len(paths) < 2 {
		t.Fatalf("got %d requests, expected the names to be split across several", len(paths))
	}

	sent := make([]string, 0, len(paths))
	for _, p := range paths {
		if len(p) > deleteURIBudget {
			t.Errorf("got a request path of %d bytes, expected at most %d", len(p), deleteURIBudget)
		}
		sent = append(sent, strings.Split(strings.TrimPrefix(p, "/"), ",")...)
	}
	if !reflect.DeepEqual(sent, names) {
		t.Errorf("got %d names across %d requests, expected all %d in order", len(sent), len(paths), len(names))
	}
}

// TestDeleteBatchedOversizedName checks that a name longer than the budget is
// still sent, one request to itself, rather than looping forever.
func TestDeleteBatchedOversizedName(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	clusterURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	names := []string{strings.Repeat("n", deleteURIBudget+1), "small"}
	if err := newClient(srv.Client(), clusterURL).deleteBatched("/", names, "drop failed"); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Errorf("got %d requests, expected 2", requests)
	}
}
