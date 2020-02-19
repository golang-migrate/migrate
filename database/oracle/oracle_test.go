package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/godror/godror"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
)

var (
	opts  = dktest.Options{PortRequired: true, ReadyFunc: isReady, Timeout: time.Minute * 40, ReadyTimeout: time.Minute * 3, PullTimeout: time.Minute * 30}
	specs = []dktesting.ContainerSpec{
		{ImageName: "maxnilz/oracle-xe:18c", Options: opts},
	}
)

type dsnFunc func(t *testing.T, args ...interface{}) string

func oracleEnvDsn(t *testing.T, _ ...interface{}) string {
	//E.g: oci8://user/password@localhost:1521/ORCLPDB1
	dsn := os.Getenv("MIGRATE_TEST_ORACLE_DSN")
	if dsn == "" {
		t.Skip("MIGRATE_TEST_ORACLE_DSN not found, skip the test case")
	}
	return dsn
}

func isDKHonored(t *testing.T) {
	s := os.Getenv("MIGRATE_TEST_ENABLE_ORACLE_CONTAINER")
	if s != "true" {
		t.Skip("MIGRATE_TEST_ENABLE_ORACLE_CONTAINER not found, skip the dk test case")
	}
}

func oracleDKDsn(t *testing.T, args ...interface{}) string {
	c := args[0].(dktest.ContainerInfo)
	ip, port, err := c.FirstPort()
	if err != nil {
		t.Fatal(err)
	}
	return oracleConnectionString(ip, port)
}

func oracleConnectionString(host, port string) string {
	return fmt.Sprintf("oracle://oracle:oracle@%s:%s/XEPDB1", host, port)
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
		queries, err := parseStatements(bytes.NewBufferString(c.migration), &Config{PLSQLStatementSeparator: plsqlDefaultStatementSeparator})
		require.Nil(t, err)
		require.Equal(t, c.expectedQueries, queries)
	}
}

func TestOpen(t *testing.T) {
	testOpen(t, oracleEnvDsn)
}

func TestMigrate(t *testing.T) {
	testMigrate(t, oracleEnvDsn)
}

func TestLockWorks(t *testing.T) {
	testLockWorks(t, oracleEnvDsn)
}

func TestWithInstanceConcurrent(t *testing.T) {
	testWithInstanceConcurrent(t, oracleEnvDsn)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}
	db, err := sql.Open("godror", oracleConnectionString(ip, port))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		oraErr, ok := godror.AsOraErr(err)
		if ok && (oraErr.Code() == 12514 || oraErr.Code() == 12537 || oraErr.Code() == 12547) {
			// log the not ready very 60s
			if time.Now().Unix()%60 == 0 {
				log.Println(oracleConnectionString(ip, port), "not ready")
			}
		} else {
			log.Println(err)
		}
		return false
	}

	return true
}

// Since start a oracle container is very time expensive, just try start one and reuse it for different test case.
func TestAllInOneWithDK(t *testing.T) {
	isDKHonored(t)
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		testOpen(t, oracleDKDsn, c)
		testMigrate(t, oracleDKDsn, c)
		testLockWorks(t, oracleDKDsn, c)
		testWithInstanceConcurrent(t, oracleDKDsn, c)
	})
}

func testOpen(t *testing.T, oracleDsnFunc dsnFunc, args ...interface{}) {
	ora := &Oracle{}
	d, err := ora.Open(oracleDsnFunc(t, args...))
	require.Nil(t, err)
	require.NotNil(t, d)
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	ora = d.(*Oracle)
	require.Equal(t, defaultMigrationsTable, ora.config.MigrationsTable)

	tbName := ""
	err = ora.conn.QueryRowContext(context.Background(), `SELECT tname FROM tab WHERE tname = :1`, ora.config.MigrationsTable).Scan(&tbName)
	require.Nil(t, err)
	require.Equal(t, ora.config.MigrationsTable, tbName)

	dt.Test(t, d, []byte(`BEGIN DBMS_OUTPUT.PUT_LINE('hello'); END;`))
}

func testMigrate(t *testing.T, oracleDsnFunc dsnFunc, args ...interface{}) {
	p := &Oracle{}
	d, err := p.Open(oracleDsnFunc(t, args...))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "", d)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)
}

func testLockWorks(t *testing.T, oracleDsnFunc dsnFunc, args ...interface{}) {
	p := &Oracle{}
	d, err := p.Open(oracleDsnFunc(t, args...))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()

	dt.Test(t, d, []byte(`BEGIN DBMS_OUTPUT.PUT_LINE('hello'); END;`))

	ora := d.(*Oracle)

	err = ora.Lock()
	if err != nil {
		t.Fatal(err)
	}

	err = ora.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	err = ora.Lock()
	if err != nil {
		t.Fatal(err)
	}

	err = ora.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func testWithInstanceConcurrent(t *testing.T, oracleDsnFunc dsnFunc, args ...interface{}) {
	// The number of concurrent processes running WithInstance
	const concurrency = 30

	// We can instantiate a single database handle because it is
	// actually a connection pool, and so, each of the below go
	// routines will have a high probability of using a separate
	// connection, which is something we want to exercise.
	db, err := sql.Open("godror", oracleDsnFunc(t, args...))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	db.SetMaxIdleConns(concurrency)
	db.SetMaxOpenConns(concurrency)

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := WithInstance(db, &Config{})
			if err != nil {
				t.Errorf("process %d error: %s", i, err)
			}
		}(i)
	}
}
