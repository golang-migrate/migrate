package oracle

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dhui/dktest"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	defaultPort = 1521
	userdba     = "orcl"
	userdbaPass = "orcl"
	defaultPass = "orcl"
)

var (
	specs = []dktesting.ContainerSpec{
		{
			ImageName: "gvenzl/oracle-free:23.5-slim", Options: oracleOptions(),
		},
	}
)

func oracleOptions() dktest.Options {
	cwd, _ := os.Getwd()
	mounts := []mount.Mount{
		{
			Type:   mount.TypeBind,
			Source: filepath.Join(cwd, "testdata/init.sql"),
			Target: "/docker-entrypoint-initdb.d/init.sql",
		},
	}

	return dktest.Options{
		PortRequired: true,
		Mounts:       mounts,
		ReadyFunc:    isReady,
		ExposedPorts: nat.PortSet{
			nat.Port(fmt.Sprintf("%d/tcp", defaultPort)): {},
		},
		PortBindings: map[nat.Port][]nat.PortBinding{
			nat.Port(fmt.Sprintf("%d/tcp", defaultPort)): {
				nat.PortBinding{
					HostIP:   "0.0.0.0",
					HostPort: "0",
				},
			},
		},
		Env: map[string]string{
			"ORACLE_PASSWORD": defaultPass,
		},
	}
}

func oracleConnectionString(host, port string) string {
	return fmt.Sprintf("oracle://%s:%s@%s:%s/FREEPDB1", userdba, userdbaPass, host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("oracle", oracleConnectionString(ip, port))
	if err != nil {
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
			fmt.Println(err)
		}
		return false
	}
	return true
}

type oracleSuite struct {
	dsn string
	suite.Suite
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestOracleTestSuite(t *testing.T) {
	if dsn := os.Getenv("ORACLE_DSN"); dsn != "" {
		s := oracleSuite{dsn: dsn}
		suite.Run(t, &s)
		return
	}

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		dsn := oracleConnectionString(ip, port)
		s := oracleSuite{dsn: dsn}

		suite.Run(t, &s)
	})
}

func (s *oracleSuite) TestMigrate() {
	ora := &Oracle{}
	d, err := ora.Open(s.dsn)
	s.Require().Nil(err)
	s.Require().NotNil(d)

	defer func() {
		if err := d.Close(); err != nil {
			s.Error(err)
		}
	}()

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "", d)
	s.Require().Nil(err)
	dt.TestMigrate(s.T(), m)
}

func (s *oracleSuite) TestMultiStmtMigrate() {
	ora := &Oracle{}
	dsn := fmt.Sprintf("%s?%s=%s&&%s=%s", s.dsn, multiStmtEnableQueryKey, "true", multiStmtSeparatorQueryKey, "---")
	d, err := ora.Open(dsn)
	s.Require().Nil(err)
	s.Require().NotNil(d)
	defer func() {
		if err := d.Close(); err != nil {
			s.Error(err)
		}
	}()
	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations-multistmt", "", d)
	s.Require().Nil(err)
	dt.TestMigrate(s.T(), m)
}

func (s *oracleSuite) TestLockWorks() {
	ora := &Oracle{}
	d, err := ora.Open(s.dsn)
	s.Require().Nil(err)
	s.Require().NotNil(d)
	defer func() {
		if err := d.Close(); err != nil {
			s.Error(err)
		}
	}()

	dt.Test(s.T(), d, []byte(`BEGIN DBMS_OUTPUT.PUT_LINE('hello'); END;`))

	ora = d.(*Oracle)
	err = ora.Lock()
	s.Require().Nil(err)

	err = ora.Unlock()
	s.Require().Nil(err)

	err = ora.Lock()
	s.Require().Nil(err)

	err = ora.Unlock()
	s.Require().Nil(err)
}

func TestOpen_InvalidURL(t *testing.T) {
	ora := &Oracle{}
	_, err := ora.Open(":\x00invalid")
	require.Error(t, err)
}

func TestOpen_CustomParams(t *testing.T) {
	cases := []struct {
		name          string
		url           string
		wantTable     string
		wantMultiStmt bool
		wantSeparator string
	}{
		{
			name:          "default values when no params",
			url:           "oracle://user:pass@localhost:1521/FREEPDB1",
			wantTable:     DefaultMigrationsTable,
			wantMultiStmt: DefaultMultiStmtEnabled,
			wantSeparator: DefaultMultiStmtSeparator,
		},
		{
			name:          "custom migrations table",
			url:           "oracle://user:pass@localhost:1521/FREEPDB1?x-migrations-table=my_migrations",
			wantTable:     "MY_MIGRATIONS",
			wantMultiStmt: DefaultMultiStmtEnabled,
			wantSeparator: DefaultMultiStmtSeparator,
		},
		{
			name:          "multi stmt enabled",
			url:           "oracle://user:pass@localhost:1521/FREEPDB1?x-multi-stmt-enabled=true",
			wantTable:     DefaultMigrationsTable,
			wantMultiStmt: true,
			wantSeparator: DefaultMultiStmtSeparator,
		},
		{
			// URL query string: "x-multi-stmt-separator===" — the first "=" is the
			// key=value delimiter, so the parsed value is "==".
			name:          "custom separator",
			url:           "oracle://user:pass@localhost:1521/FREEPDB1?x-multi-stmt-separator===",
			wantTable:     DefaultMigrationsTable,
			wantMultiStmt: DefaultMultiStmtEnabled,
			wantSeparator: "==",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			purl, err := neturl.Parse(tc.url)
			require.NoError(t, err)
			cfg, err := parseURLParams(purl)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTable, cfg.MigrationsTable)
			assert.Equal(t, tc.wantMultiStmt, cfg.MultiStmtEnabled)
			assert.Equal(t, tc.wantSeparator, cfg.MultiStmtSeparator)
		})
	}
}

func TestOpen_InvalidMultiStmtEnabled(t *testing.T) {
	purl, err := neturl.Parse("oracle://user:pass@localhost:1521/FREEPDB1?x-multi-stmt-enabled=notabool")
	require.NoError(t, err)
	_, err = parseURLParams(purl)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "x-multi-stmt-enabled")
}

func TestOpen_InvalidMigrationsTable(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{
			name: "single quote injection",
			url:  "oracle://user:pass@localhost:1521/FREEPDB1?x-migrations-table=O'TABLE",
		},
		{
			name: "starts with digit",
			url:  "oracle://user:pass@localhost:1521/FREEPDB1?x-migrations-table=1TABLE",
		},
		{
			name: "contains space",
			url:  "oracle://user:pass@localhost:1521/FREEPDB1?x-migrations-table=MY%20TABLE",
		},
		{
			name: "semicolon injection",
			url:  "oracle://user:pass@localhost:1521/FREEPDB1?x-migrations-table=T%3BDROP",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			purl, err := neturl.Parse(tc.url)
			require.NoError(t, err)
			_, err = parseURLParams(purl)
			require.Error(t, err)
		})
	}
}

func TestRemoveComments(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
		{
			name:     "only comments",
			input:    "-- comment\n-- another comment\n",
			expected: "",
		},
		{
			name:     "mix of comments and sql",
			input:    "-- comment\nSELECT 1\n-- another\nFROM DUAL\n",
			expected: "SELECT 1\nFROM DUAL\n",
		},
		{
			name:     "no comments",
			input:    "SELECT 1\nFROM DUAL\n",
			expected: "SELECT 1\nFROM DUAL\n",
		},
		{
			name:     "inline non-comment dash",
			input:    "SELECT 1 - 1\n",
			expected: "SELECT 1 - 1\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := removeComments(strings.NewReader(tc.input))
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseStatements(t *testing.T) {
	cases := []struct {
		migration       string
		expectedQueries []string
	}{
		{migration: `
CREATE TABLE USERS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
);

---
--
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE USERS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;

---
-- comment
--
CREATE TABLE USERS (
   USER_ID integer unique,
   NAME    varchar(40),
   EMAIL   varchar(40)
);
---
--`,
			expectedQueries: []string{
				`CREATE TABLE USERS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
)`,
				`BEGIN
EXECUTE IMMEDIATE 'DROP TABLE USERS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;`,
				`CREATE TABLE USERS (
   USER_ID integer unique,
   NAME    varchar(40),
   EMAIL   varchar(40)
)`,
			}},
		{migration: `
-- comment
CREATE TABLE USERS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
);
-- this is comment
---
ALTER TABLE USERS ADD CITY varchar(100);
`,
			expectedQueries: []string{
				`CREATE TABLE USERS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
)`,
				`ALTER TABLE USERS ADD CITY varchar(100)`,
			}},
	}
	for _, c := range cases {
		queries, err := parseMultiStatements(bytes.NewBufferString(c.migration), DefaultMultiStmtSeparator)
		require.Nil(t, err)
		require.Equal(t, c.expectedQueries, queries)
	}
}
