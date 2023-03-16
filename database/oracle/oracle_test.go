package oracle

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type oracleSuite struct {
	suite.Suite
	dsn       string
	container testcontainers.Container
}

func (s *oracleSuite) SetupSuite() {
	dsn := os.Getenv("ORACLE_DSN")
	if dsn != "" {
		s.dsn = dsn
		return
	}

	username := "orcl"
	password := "orcl"
	db := "XEPDB1"
	nPort, err := nat.NewPort("tcp", "1521")
	if err != nil {
		return
	}
	cwd, _ := os.Getwd()
	req := testcontainers.ContainerRequest{
		Image:        "container-registry.oracle.com/database/express:18.4.0-xe",
		ExposedPorts: []string{nPort.Port()},
		Env: map[string]string{
			// password for SYS and SYSTEM users
			"ORACLE_PWD": password,
		},
		Mounts: testcontainers.ContainerMounts{
			testcontainers.BindMount(filepath.Join(cwd, "testdata/user.sql"), "/opt/oracle/scripts/setup/user.sql"),
		},
		WaitingFor: wait.NewHealthStrategy().WithStartupTimeout(time.Minute * 15),
		AutoRemove: true,
	}

	ctx := context.Background()
	orcl, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.Require().NoError(err)
	host, err := orcl.Host(ctx)
	s.Require().NoError(err)
	mappedPort, err := orcl.MappedPort(ctx, nPort)
	s.Require().NoError(err)
	port := mappedPort.Port()

	s.dsn = fmt.Sprintf("oracle://%s:%s@%s:%s/%s", username, password, host, port, db)
	s.container = orcl
}

func (s *oracleSuite) TearDownSuite() {
	if s.container != nil {
		_ = s.container.Terminate(context.Background())
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestOracleTestSuite(t *testing.T) {
	suite.Run(t, new(oracleSuite))
}

func (s *oracleSuite) TestOpen() {
	ora := &Oracle{}
	d, err := ora.Open(s.dsn)
	s.Require().Nil(err)
	s.Require().NotNil(d)
	defer func() {
		if err := d.Close(); err != nil {
			s.Error(err)
		}
	}()
	ora = d.(*Oracle)
	s.Require().Equal(DefaultMigrationsTable, ora.config.MigrationsTable)

	tbName := ""
	err = ora.conn.QueryRowContext(
		context.Background(),
		`SELECT tname FROM tab WHERE tname = :1`,
		ora.config.MigrationsTable,
	).Scan(&tbName)
	s.Require().Nil(err)
	s.Require().Equal(ora.config.MigrationsTable, tbName)

	dt.Test(s.T(), d, []byte(`BEGIN DBMS_OUTPUT.PUT_LINE('hello'); END;`))
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
