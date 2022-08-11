package snowflake

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang-migrate/migrate/v4/database"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	wantMigrationTable = "WANT_MIGRATION_TABLE"
	wantDatabase       = "WANT_DATABASE"
)

// expectErrorsEqual validates that the `got` error is equal to the `want` error, or that got.Error() contains `want` if `want` is a string
func expectErrorsEqual(t *testing.T, got error, want interface{}) {
	if got == nil && want != nil {
		t.Fatalf("expected an error but received none")
	}
	if got != nil && want == nil {
		t.Fatalf("expected no error but received [%s]", got)
	}
	if wantStr, isStr := want.(string); isStr && !strings.Contains(got.Error(), wantStr) {
		t.Fatalf("expected error [%s] to contain [%s]", got.Error(), wantStr)
	} else if wantErr, isErr := want.(error); isErr && wantErr.Error() != got.Error() {
		t.Fatalf("expected error [%s] but received [%s]", got.Error(), wantErr.Error())
	}
}

// mockSnowflakeDB creates a Snowflake instance that is backed by a `mock` db.  Call `finish` after your test to verify
// mock expectations and cleanup
func mockSnowflakeDB(t *testing.T) (db *Snowflake, mock sqlmock.Sqlmock, finish func()) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("creating mock db: %s", err)
	}
	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("creating mock conn: %s", err)
	}
	db = &Snowflake{
		conn: conn,
		db:   sqlDB,
		config: &Config{
			MigrationsTable: wantMigrationTable,
			DatabaseName:    wantDatabase,
		},
	}
	finish = func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
		mock.ExpectClose()
		_ = db.Close()
	}
	return
}

func TestConfigForURL(t *testing.T) {
	tests := []struct {
		name       string
		inputURL   string      // the URL supplied to configForURL()
		wantConfig *Config     // the expected config that should be returned
		wantError  interface{} // the error, or portion of the error message, that should be returned
	}{
		{
			name:     "it should populate a config from a valid url",
			inputURL: "snowflake://user:password@accountname/schema/dbname?query",
			wantConfig: &Config{
				MigrationsTable: "",
				DatabaseName:    "dbname",
				ConnectTimeout:  DefaultConnectTimeout,
				dsn:             "user:password@accountname.snowflakecomputing.com:443?database=dbname&ocspFailOpen=true&requestTimeout=300&schema=schema&validateDefaultParameters=true",
			},
			wantError: nil,
		},
		{
			name:     "it should configure the migrations table from query parameters",
			inputURL: "snowflake://user:password@accountname/schema/dbname?x-migrations-table=migrations",
			wantConfig: &Config{
				MigrationsTable: "migrations",
				DatabaseName:    "dbname",
				ConnectTimeout:  DefaultConnectTimeout,
				dsn:             "user:password@accountname.snowflakecomputing.com:443?database=dbname&ocspFailOpen=true&requestTimeout=300&schema=schema&validateDefaultParameters=true",
			},
			wantError: nil,
		},
		{
			name:     "it should configure the connect timeout from query parameters",
			inputURL: "snowflake://user:password@accountname/schema/dbname?x-connect-timeout=72",
			wantConfig: &Config{
				MigrationsTable: "",
				DatabaseName:    "dbname",
				ConnectTimeout:  72 * time.Second,
				dsn:             "user:password@accountname.snowflakecomputing.com:443?database=dbname&ocspFailOpen=true&requestTimeout=300&schema=schema&validateDefaultParameters=true",
			},
			wantError: nil,
		},
		{
			name:     "it should configure the request timeout from query parameters",
			inputURL: "snowflake://user:password@accountname/schema/dbname?x-timeout=72",
			wantConfig: &Config{
				MigrationsTable: "",
				DatabaseName:    "dbname",
				ConnectTimeout:  DefaultConnectTimeout,
				dsn:             "user:password@accountname.snowflakecomputing.com:443?database=dbname&ocspFailOpen=true&requestTimeout=72&schema=schema&validateDefaultParameters=true",
			},
			wantError: nil,
		},
		{
			name:     "it should configure warehouse and role from query parameters",
			inputURL: "snowflake://user:password@accountname/schema/dbname?x-warehouse=wh&x-role=role",
			wantConfig: &Config{
				MigrationsTable: "",
				DatabaseName:    "dbname",
				ConnectTimeout:  DefaultConnectTimeout,
				dsn:             "user:password@accountname.snowflakecomputing.com:443?database=dbname&ocspFailOpen=true&requestTimeout=300&role=role&schema=schema&validateDefaultParameters=true&warehouse=wh",
			},
			wantError: nil,
		},
		{
			name:     "it should enable multi-statement from query parameters",
			inputURL: "snowflake://user:password@accountname/schema/dbname?x-multi-statement=true",
			wantConfig: &Config{
				MigrationsTable:       "",
				DatabaseName:          "dbname",
				MultiStatementEnabled: true,
				ConnectTimeout:        DefaultConnectTimeout,
				dsn:                   "user:password@accountname.snowflakecomputing.com:443?database=dbname&ocspFailOpen=true&requestTimeout=300&schema=schema&validateDefaultParameters=true",
			},
			wantError: nil,
		},
		{
			name:      "it should error if x-multi-statement is not a boolean",
			inputURL:  "snowflake://user:password@accountname/schema/dbname?x-multi-statement=foo",
			wantError: ErrInvalidParameterFormat,
		},
		{
			name:      "it should error if unable to parse the url",
			inputURL:  "fo?>??ASD:::\033obar",
			wantError: "invalid control character",
		},
		{
			name:      "it should return an error if the password is missing",
			inputURL:  "snowflake://accountname/schema/dbname?query",
			wantError: ErrNoPassword,
		},
		{
			name:      "it should return an error if the schema is missing",
			inputURL:  "snowflake://user:password@accountname//dbname?query",
			wantError: ErrNoSchema,
		},
		{
			name:      "it should return an error if the database is missing",
			inputURL:  "snowflake://user:password@accountname/schema/?query",
			wantError: ErrNoDatabaseName,
		},
		{
			name:      "it should return an error if the database and schema are missing",
			inputURL:  "snowflake://user:password@accountname/?query",
			wantError: ErrNoSchemaOrDatabase,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotConfig, gotErr := configForURL(test.inputURL)
			expectErrorsEqual(t, gotErr, test.wantError)
			if test.wantConfig != nil && !reflect.DeepEqual(test.wantConfig, gotConfig) {
				t.Fatalf("expected [%+v] but got [%+v]", test.wantConfig, gotConfig)
			}
		})
	}
}

func TestSetVersion(t *testing.T) {
	tests := []struct {
		name            string
		inputVersion    int                        // the version value supplied to SetVersion()
		inputDirty      bool                       // the dirty value supplied to SetVersion()
		setExpectations func(mock sqlmock.Sqlmock) // a helper to set the expected database operations for this test
		wantError       interface{}                // the error, or portion of the error message, that should be returned
	}{
		{
			name:         "it should delete from the table and insert a new dirty version",
			inputVersion: 10,
			inputDirty:   true,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(`INSERT INTO "WANT_MIGRATION_TABLE" (version, dirty) VALUES (10, true)`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			wantError: nil,
		},
		{
			name:         "it should delete from the table and insert a new clean version",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(`INSERT INTO "WANT_MIGRATION_TABLE" (version, dirty) VALUES (25, false)`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			wantError: nil,
		},
		{
			name:         "it should return an error if the commit fails",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(`INSERT INTO "WANT_MIGRATION_TABLE" (version, dirty) VALUES (25, false)`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit().WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
		{
			name:         "it should rollback if the delete fails",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnError(errors.New("delete error"))
				mock.ExpectRollback()
			},
			wantError: "delete error",
		},
		{
			name:         "it should rollback if the insert fails",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(`INSERT INTO "WANT_MIGRATION_TABLE" (version, dirty) VALUES (25, false)`).
					WillReturnError(errors.New("insert error"))
				mock.ExpectRollback()
			},
			wantError: "insert error",
		},
		{
			name:         "it should return an error if unable to begin a transaction",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("begin error"))
			},
			wantError: "begin error",
		},
		{
			name:         "it should return an error if the delete rollback fails",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnError(errors.New("delete error"))
				mock.ExpectRollback().
					WillReturnError(errors.New("rollback error"))
			},
			wantError: "rollback error",
		},
		{
			name:         "it should return an error if the insert rollback fails",
			inputVersion: 25,
			inputDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(`DELETE FROM "WANT_MIGRATION_TABLE"`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(`INSERT INTO "WANT_MIGRATION_TABLE" (version, dirty) VALUES (25, false)`).
					WillReturnError(errors.New("insert error"))
				mock.ExpectRollback().
					WillReturnError(errors.New("rollback error"))
			},
			wantError: "rollback error",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, finish := mockSnowflakeDB(t)
			defer finish()
			test.setExpectations(mock)
			gotErr := db.SetVersion(test.inputVersion, test.inputDirty)
			expectErrorsEqual(t, gotErr, test.wantError)
		})
	}

}

func TestVersion(t *testing.T) {
	tests := []struct {
		name            string
		setExpectations func(mock sqlmock.Sqlmock) // a helper to set the expected database operations for this test
		wantVersion     int                        // the version that should be returned
		wantDirty       bool                       // the dirty value that should be returned
		wantError       interface{}                // the error, or portion of the error message, that should be returned
	}{
		{
			name:        "it should return the version from the database",
			wantVersion: 10,
			wantDirty:   true,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT version, dirty FROM "WANT_MIGRATION_TABLE" LIMIT 1`).
					WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).
						AddRow(10, true))
			},
			wantError: nil,
		},
		{
			name:        "it should return a nil version if there are no versions in the database",
			wantVersion: database.NilVersion,
			wantDirty:   false,
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT version, dirty FROM "WANT_MIGRATION_TABLE" LIMIT 1`).
					WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}))
			},
			wantError: nil,
		},
		{
			name: "it should return an error if the query fails",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT version, dirty FROM "WANT_MIGRATION_TABLE" LIMIT 1`).
					WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
		{
			name: "it should return a nil version if snowflake reports the object doesn't exist",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT version, dirty FROM "WANT_MIGRATION_TABLE" LIMIT 1`).
					WillReturnError(&sf.SnowflakeError{Number: sf.ErrObjectNotExistOrAuthorized})
			},
			wantVersion: database.NilVersion,
			wantDirty:   false,
			wantError:   nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, finish := mockSnowflakeDB(t)
			defer finish()
			test.setExpectations(mock)
			gotVersion, gotDirty, gotErr := db.Version()
			expectErrorsEqual(t, gotErr, test.wantError)
			if gotErr == nil && gotVersion != test.wantVersion {
				t.Fatalf("expected version to be [%d] but got [%d]", test.wantVersion, gotVersion)
			}
			if gotErr == nil && gotDirty != test.wantDirty {
				t.Fatalf("expected dirty to be [%v] but got [%v]", test.wantDirty, gotDirty)
			}
		})
	}

}

func TestDrop(t *testing.T) {
	tests := []struct {
		name            string
		setExpectations func(mock sqlmock.Sqlmock) // a helper to set the expected database operations for this test
		wantError       interface{}                // the error, or portion of the error message, that should be returned from this test
	}{
		{
			name: "it should drop all tables in the schema",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`).
					WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("foo").
						AddRow("bar"))
				mock.ExpectExec(`DROP TABLE IF EXISTS foo CASCADE`).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(`DROP TABLE IF EXISTS bar CASCADE`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantError: nil,
		},
		{
			name: "it should error if the table query fails",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`).
					WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
		{
			name: "it should error if the drop fails",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`).
					WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("foo").
						AddRow("bar"))
				mock.ExpectExec(`DROP TABLE IF EXISTS foo CASCADE`).
					WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, finish := mockSnowflakeDB(t)
			defer finish()
			test.setExpectations(mock)
			gotErr := db.Drop()
			expectErrorsEqual(t, gotErr, test.wantError)
		})
	}

}

func TestClose(t *testing.T) {
	tests := []struct {
		name            string
		setExpectations func(mock sqlmock.Sqlmock) // a helper to set the expected database operations for this test
		wantError       interface{}                // the error, or portion of the error message, that should be returned from this test
	}{
		{
			name: "it should close the database",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectClose()
			},
			wantError: nil,
		},
		{
			name: "it should return an error if unable to close the database",
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectClose().WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, finish := mockSnowflakeDB(t)
			defer finish()
			test.setExpectations(mock)
			gotErr := db.Close()
			expectErrorsEqual(t, gotErr, test.wantError)
		})
	}
}

var _ io.Reader = (*errReader)(nil)

type errReader struct{}

func (e errReader) Read(_ []byte) (n int, err error) {
	return 0, errors.New("read error")
}

func TestRun(t *testing.T) {
	tests := []struct {
		name            string
		inputReader     io.Reader                  // the reader supplied to Run()
		setExpectations func(mock sqlmock.Sqlmock) // a helper to set the expected database operations for this test
		wantError       interface{}                // the error, or portion of the error message, that should be returned
	}{
		{
			name:        "it should execute the contents of the migration",
			inputReader: strings.NewReader("DELETE FROM foo"),
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`DELETE FROM foo`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantError: nil,
		},
		{
			name:        "it should return an error if the query fails",
			inputReader: strings.NewReader("DELETE FROM foo"),
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`DELETE FROM foo`).
					WillReturnError(errors.New("bar"))
			},
			wantError: "bar",
		},
		{
			name:            "it should return an error if unable to read from the reader",
			inputReader:     &errReader{},
			setExpectations: func(mock sqlmock.Sqlmock) {},
			wantError:       "read error",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, finish := mockSnowflakeDB(t)
			defer finish()
			test.setExpectations(mock)
			gotErr := db.Run(test.inputReader)
			expectErrorsEqual(t, gotErr, test.wantError)
		})
	}

}

func TestWithInstance(t *testing.T) {
	tests := []struct {
		name            string
		inputConfig     *Config                    // the config passed to WithInstance
		setExpectations func(mock sqlmock.Sqlmock) // a helper to set the expected database operations for this test
		wantConfig      *Config                    // (optional) the expected config returned from WithInstance
		wantError       interface{}                // the error, or portion of the error message, that should be returned
	}{
		{
			name:            "it should error if no config is specified",
			inputConfig:     nil,
			setExpectations: func(mock sqlmock.Sqlmock) {},
			wantError:       ErrNilConfig,
		},
		{
			name: "it should error if unable to ping the database",
			inputConfig: &Config{
				ConnectTimeout: DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing().WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
		{
			name: "it should determine default migration table and database",
			inputConfig: &Config{
				ConnectTimeout: DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				mock.ExpectQuery(`SELECT CURRENT_DATABASE()`).
					WillReturnRows(sqlmock.NewRows([]string{"CURRENT_DATABASE"}).
						AddRow("FOO"))

				mock.ExpectQuery(`SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`).
					WithArgs(DefaultMigrationsTable).
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).
						AddRow(1))
			},
			wantConfig: &Config{
				DatabaseName:    "FOO",
				MigrationsTable: DefaultMigrationsTable,
				ConnectTimeout:  DefaultConnectTimeout,
			},
			wantError: nil,
		},
		{
			name: "it should error if unable to determine the current database",
			inputConfig: &Config{
				ConnectTimeout: DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				mock.ExpectQuery(`SELECT CURRENT_DATABASE()`).
					WillReturnRows(sqlmock.NewRows([]string{"CURRENT_DATABASE"}).
						AddRow(""))
			},
			wantError: ErrNoDatabaseName,
		},
		{
			name: "it should error if the query to determine the current database fails",
			inputConfig: &Config{
				ConnectTimeout: DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				mock.ExpectQuery(`SELECT CURRENT_DATABASE()`).WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
		{
			name: "it should not create the migration table if it already exists",
			inputConfig: &Config{
				DatabaseName:    "FOO",
				MigrationsTable: "BAR",
				ConnectTimeout:  DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				mock.ExpectQuery(`SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`).
					WithArgs("BAR").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).
						AddRow(1))
			},
			wantError: nil,
		},
		{
			name: "it should create the migration table if it doesn't already exists",
			inputConfig: &Config{
				DatabaseName:    "FOO",
				MigrationsTable: "BAR",
				ConnectTimeout:  DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()

				mock.ExpectQuery(`SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`).
					WithArgs("BAR").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).
						AddRow(0))

				mock.ExpectExec(`CREATE TABLE if not exists "BAR" ( version bigint not null primary key, dirty boolean not null)`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantError: nil,
		},
		{
			name: "it should return an error if unable to query for the migration table",
			inputConfig: &Config{
				DatabaseName:    "FOO",
				MigrationsTable: "BAR",
				ConnectTimeout:  DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				mock.ExpectQuery(`SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`).
					WithArgs("BAR").
					WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
		{
			name: "it should return an error if unable to create the migration table",
			inputConfig: &Config{
				DatabaseName:    "FOO",
				MigrationsTable: "BAR",
				ConnectTimeout:  DefaultConnectTimeout,
			},
			setExpectations: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
				mock.ExpectQuery(`SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`).
					WithArgs("BAR").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).
						AddRow(0))

				mock.ExpectExec(`CREATE TABLE if not exists "BAR" ( version bigint not null primary key, dirty boolean not null)`).
					WillReturnError(errors.New("foo"))
			},
			wantError: "foo",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(
				sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
				sqlmock.MonitorPingsOption(true))

			if err != nil {
				t.Fatalf("creating mock db: %s", err)
			}
			test.setExpectations(mock)
			gotSF, gotErr := WithInstance(db, test.inputConfig)

			expectErrorsEqual(t, gotErr, test.wantError)

			if test.wantConfig != nil && !reflect.DeepEqual(test.wantConfig, gotSF.(*Snowflake).config) {
				t.Fatal("config not expected")
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations: %s", err)
			}
		})
	}

}

func TestLocking(t *testing.T) {
	s := &Snowflake{}
	if err := s.Lock(); err != nil {
		t.Fatalf("should not error if currently unlocked")
	}
	if err := s.Lock(); err != database.ErrLocked {
		t.Fatalf("expected database lock error but received [%s]", err)
	}
	if err := s.Unlock(); err != nil {
		t.Fatalf("should not error if currently locked")
	}
	if err := s.Unlock(); err != database.ErrNotLocked {
		t.Fatalf("expected database not locked error but received [%s]", err)
	}
}
