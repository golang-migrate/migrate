package oracle

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dhui/dktest"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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
					HostPort: "0/tcp",
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
