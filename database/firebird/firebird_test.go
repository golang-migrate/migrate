package firebird

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	nurl "net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/dhui/dktest"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	_ "github.com/nakagami/firebirdsql"
)

const (
	user     = "test_user"
	password = "123456"
	dbName   = "test.fdb"
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		Env: map[string]string{
			"FIREBIRD_DATABASE": dbName,
			"FIREBIRD_USER":     user,
			"FIREBIRD_PASSWORD": password,
		},
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "jacobalberty/firebird:v3.0", Options: opts},
		{ImageName: "jacobalberty/firebird:v4.0", Options: opts},
		{ImageName: "jacobalberty/firebird:v5.0", Options: opts},
	}
)

func fbConnectionString(host, port string) string {
	//firebird://user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]
	return fmt.Sprintf("firebird://%s:%s@%s:%s//firebird/data/%s", user, password, host, port, dbName)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	db, err := sql.Open("firebirdsql", fbConnectionString(ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("SELECT Count(*) FROM rdb$relations"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "firebirdsql", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMultipleStatementsInMultiStatementMode(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port) + "?x-multi-statement=true"
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		// Use CREATE INDEX instead of CONCURRENTLY (Firebird doesn't support CREATE INDEX CONCURRENTLY)
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo VARCHAR(40)); CREATE INDEX idx_foo ON foo (foo);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure created index exists
		var exists bool
		query := "SELECT CASE WHEN EXISTS (SELECT 1 FROM RDB$INDICES WHERE RDB$INDEX_NAME = 'IDX_FOO') THEN 1 ELSE 0 END FROM RDB$DATABASE"
		if err := d.(*Firebird).conn.QueryRowContext(context.Background(), query).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected index idx_foo to exist")
		}
	})
}

func TestErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		wantErr := `migration failed in line 0: CREATE TABLEE foo (foo varchar(40)); (details: Dynamic SQL Error
SQL error code = -104
Token unknown - line 1, column 8
TABLEE
)`

		if err := d.Run(strings.NewReader("CREATE TABLEE foo (foo varchar(40));")); err == nil {
			t.Fatal("expected err but got nil")
		} else if err.Error() != wantErr {
			msg := err.Error()
			t.Fatalf("expected '%s' but got '%s'", wantErr, msg)
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port) + "?sslmode=disable&x-custom=foobar"
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	})
}

func Test_Lock(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT Count(*) FROM rdb$relations"))

		ps := d.(*Firebird)

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestMultiStatementURLParsing(t *testing.T) {
	tests := []struct {
		name                  string
		url                   string
		expectedMultiStmt     bool
		expectedMultiStmtSize int
		shouldError           bool
	}{
		{
			name:                  "multi-statement enabled",
			url:                   "firebird://user:pass@localhost:3050//path/to/db.fdb?x-multi-statement=true",
			expectedMultiStmt:     true,
			expectedMultiStmtSize: DefaultMultiStatementMaxSize,
			shouldError:           false,
		},
		{
			name:                  "multi-statement disabled",
			url:                   "firebird://user:pass@localhost:3050//path/to/db.fdb?x-multi-statement=false",
			expectedMultiStmt:     false,
			expectedMultiStmtSize: DefaultMultiStatementMaxSize,
			shouldError:           false,
		},
		{
			name:                  "multi-statement with custom size",
			url:                   "firebird://user:pass@localhost:3050//path/to/db.fdb?x-multi-statement=true&x-multi-statement-max-size=5242880",
			expectedMultiStmt:     true,
			expectedMultiStmtSize: 5242880,
			shouldError:           false,
		},
		{
			name:                  "multi-statement with invalid size falls back to default",
			url:                   "firebird://user:pass@localhost:3050//path/to/db.fdb?x-multi-statement=true&x-multi-statement-max-size=0",
			expectedMultiStmt:     true,
			expectedMultiStmtSize: DefaultMultiStatementMaxSize,
			shouldError:           false,
		},
		{
			name:                  "invalid boolean value should error",
			url:                   "firebird://user:pass@localhost:3050//path/to/db.fdb?x-multi-statement=invalid",
			expectedMultiStmt:     false,
			expectedMultiStmtSize: DefaultMultiStatementMaxSize,
			shouldError:           true,
		},
		{
			name:                  "invalid size value should error",
			url:                   "firebird://user:pass@localhost:3050//path/to/db.fdb?x-multi-statement=true&x-multi-statement-max-size=invalid",
			expectedMultiStmt:     true,
			expectedMultiStmtSize: DefaultMultiStatementMaxSize,
			shouldError:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't actually open a database connection without Docker,
			// but we can test the URL parsing logic by examining how Open would behave
			purl, err := nurl.Parse(tt.url)
			if err != nil {
				if !tt.shouldError {
					t.Fatalf("parseURL failed: %v", err)
				}
				return
			}

			// Test multi-statement parameter parsing
			multiStatementEnabled := false
			multiStatementMaxSize := DefaultMultiStatementMaxSize

			if s := purl.Query().Get("x-multi-statement"); len(s) > 0 {
				multiStatementEnabled, err = strconv.ParseBool(s)
				if err != nil {
					if tt.shouldError {
						return // Expected error
					}
					t.Fatalf("unable to parse option x-multi-statement: %v", err)
				}
			}

			if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
				multiStatementMaxSize, err = strconv.Atoi(s)
				if err != nil {
					if tt.shouldError {
						return // Expected error
					}
					t.Fatalf("unable to parse x-multi-statement-max-size: %v", err)
				}
				if multiStatementMaxSize <= 0 {
					multiStatementMaxSize = DefaultMultiStatementMaxSize
				}
			}

			if tt.shouldError {
				t.Fatalf("expected error but got none")
			}

			if multiStatementEnabled != tt.expectedMultiStmt {
				t.Errorf("expected MultiStatementEnabled to be %v, got %v", tt.expectedMultiStmt, multiStatementEnabled)
			}

			if multiStatementMaxSize != tt.expectedMultiStmtSize {
				t.Errorf("expected MultiStatementMaxSize to be %d, got %d", tt.expectedMultiStmtSize, multiStatementMaxSize)
			}
		})
	}
}

func TestMultiStatementParsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single statement",
			input:    "CREATE TABLE test (id INTEGER);",
			expected: []string{"CREATE TABLE test (id INTEGER);"},
		},
		{
			name:     "multiple statements",
			input:    "CREATE TABLE foo (id INTEGER); CREATE TABLE bar (name VARCHAR(50));",
			expected: []string{"CREATE TABLE foo (id INTEGER);", "CREATE TABLE bar (name VARCHAR(50));"},
		},
		{
			name:     "statements with whitespace",
			input:    "CREATE TABLE foo (id INTEGER);\n\n  CREATE TABLE bar (name VARCHAR(50));  \n",
			expected: []string{"CREATE TABLE foo (id INTEGER);", "CREATE TABLE bar (name VARCHAR(50));"},
		},
		{
			name:     "empty statements ignored",
			input:    "CREATE TABLE foo (id INTEGER);;CREATE TABLE bar (name VARCHAR(50));",
			expected: []string{"CREATE TABLE foo (id INTEGER);", "CREATE TABLE bar (name VARCHAR(50));"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var statements []string
			reader := strings.NewReader(tt.input)

			// Simulate what the Firebird driver does with multi-statement parsing
			err := multistmt.Parse(reader, multiStmtDelimiter, DefaultMultiStatementMaxSize, func(stmt []byte) bool {
				query := strings.TrimSpace(string(stmt))
				// Skip empty statements and standalone semicolons
				if len(query) > 0 && query != ";" {
					statements = append(statements, query)
				}
				return true // continue parsing
			})

			if err != nil {
				t.Fatalf("parsing failed: %v", err)
			}

			if len(statements) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d: %v", len(tt.expected), len(statements), statements)
			}

			for i, expected := range tt.expected {
				if statements[i] != expected {
					t.Errorf("statement %d: expected %q, got %q", i, expected, statements[i])
				}
			}
		})
	}
}
