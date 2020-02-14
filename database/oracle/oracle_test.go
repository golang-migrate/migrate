package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
)

func oracleDsn(t *testing.T) string {
	//E.g: oci8://user/password@localhost:1521/ORCLPDB1
	dsn := os.Getenv("ORACLE_DSN")
	if dsn == "" {
		t.Skip("ORACLE_DSN not found, skip the test case")
	}
	return dsn
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
`, expectedQueries: []string{
			`CREATE TABLE USERS (
  USER_ID integer unique,
  NAME    varchar(40),
  EMAIL   varchar(40)
)`,
			`ALTER TABLE USERS ADD CITY varchar(100)`,
		}},
	}
	for _, c := range cases {
		queries, err := parseStatements(bytes.NewBufferString(c.migration), plsqlDefaultStatementSeparator)
		require.Nil(t, err)
		require.Equal(t, c.expectedQueries, queries)
	}
}

func TestOpen(t *testing.T) {
	ora := &Oracle{}
	d, err := ora.Open(oracleDsn(t))
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

func TestMigrate(t *testing.T) {
	p := &Oracle{}
	d, err := p.Open(oracleDsn(t))
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

func TestLockWorks(t *testing.T) {
	t.Parallel()
	p := &Oracle{}
	d, err := p.Open(oracleDsn(t))
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

func TestWithInstance_Concurrent(t *testing.T) {
	// The number of concurrent processes running WithInstance
	const concurrency = 30

	// We can instantiate a single database handle because it is
	// actually a connection pool, and so, each of the below go
	// routines will have a high probability of using a separate
	// connection, which is something we want to exercise.
	db, err := sql.Open("oci8", oracleDsn(t))
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
