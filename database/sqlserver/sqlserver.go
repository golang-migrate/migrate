package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strconv"
	"strings"

	"go.uber.org/atomic"

	"github.com/Azure/go-autorest/autorest/adal"
	mssql "github.com/denisenkom/go-mssqldb" // mssql support
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
)

func init() {
	database.Register("sqlserver", &SQLServer{})
}

// DefaultMigrationsTable is the name of the migrations table in the database
var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig                 = fmt.Errorf("no config")
	ErrNoDatabaseName            = fmt.Errorf("no database name")
	ErrNoSchema                  = fmt.Errorf("no schema")
	ErrDatabaseDirty             = fmt.Errorf("database is dirty")
	ErrMultipleAuthOptionsPassed = fmt.Errorf("both password and useMsi=true were passed.")
)

var lockErrorMap = map[mssql.ReturnStatus]string{
	-1:   "The lock request timed out.",
	-2:   "The lock request was canceled.",
	-3:   "The lock request was chosen as a deadlock victim.",
	-999: "Parameter validation or other call error.",
}

// Config for database
type Config struct {
	MigrationsTable string
	DatabaseName    string
	SchemaName      string
}

// SQL Server connection
type SQLServer struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to garantuee that config is never nil
	config *Config
}

// WithInstance returns a database instance from an already created database connection.
//
// Note that the deprecated `mssql` driver is not supported. Please use the newer `sqlserver` driver.
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT DB_NAME()`
		var databaseName string
		if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if config.SchemaName == "" {
		query := `SELECT SCHEMA_NAME()`
		var schemaName string
		if err := instance.QueryRow(query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(schemaName) == 0 {
			return nil, ErrNoSchema
		}

		config.SchemaName = schemaName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(context.Background())

	if err != nil {
		return nil, err
	}

	ss := &SQLServer{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := ss.ensureVersionTable(); err != nil {
		return nil, err
	}

	return ss, nil
}

// Open a connection to the database.
func (ss *SQLServer) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	useMsiParam := purl.Query().Get("useMsi")
	useMsi := false
	if len(useMsiParam) > 0 {
		useMsi, err = strconv.ParseBool(useMsiParam)
		if err != nil {
			return nil, err
		}
	}

	if _, isPasswordSet := purl.User.Password(); useMsi && isPasswordSet {
		return nil, ErrMultipleAuthOptionsPassed
	}

	filteredURL := migrate.FilterCustomQuery(purl).String()

	var db *sql.DB
	if useMsi {
		resource := getAADResourceFromServerUri(purl)
		tokenProvider, err := getMSITokenProvider(resource)
		if err != nil {
			return nil, err
		}

		connector, err := mssql.NewAccessTokenConnector(
			filteredURL, tokenProvider)
		if err != nil {
			return nil, err
		}

		db = sql.OpenDB(connector)

	} else {
		db, err = sql.Open("sqlserver", filteredURL)
		if err != nil {
			return nil, err
		}
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	px, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

// Close the database connection
func (ss *SQLServer) Close() error {
	connErr := ss.conn.Close()
	dbErr := ss.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// Lock creates an advisory local on the database to prevent multiple migrations from running at the same time.
func (ss *SQLServer) Lock() error {
	return database.CasRestoreOnErr(&ss.isLocked, false, true, database.ErrLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(ss.config.DatabaseName, ss.config.SchemaName)
		if err != nil {
			return err
		}

		// This will either obtain the lock immediately and return true,
		// or return false if the lock cannot be acquired immediately.
		// MS Docs: sp_getapplock: https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-2017
		query := `EXEC sp_getapplock @Resource = @p1, @LockMode = 'Update', @LockOwner = 'Session', @LockTimeout = 0`

		var status mssql.ReturnStatus
		if _, err = ss.conn.ExecContext(context.Background(), query, aid, &status); err == nil && status > -1 {
			return nil
		} else if err != nil {
			return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
		} else {
			return &database.Error{Err: fmt.Sprintf("try lock failed with error %v: %v", status, lockErrorMap[status]), Query: []byte(query)}
		}
	})
}

// Unlock froms the migration lock from the database
func (ss *SQLServer) Unlock() error {
	return database.CasRestoreOnErr(&ss.isLocked, true, false, database.ErrNotLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(ss.config.DatabaseName, ss.config.SchemaName)
		if err != nil {
			return err
		}

		// MS Docs: sp_releaseapplock: https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-releaseapplock-transact-sql?view=sql-server-2017
		query := `EXEC sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'`
		if _, err := ss.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		return nil
	})
}

// Run the migrations for the database
func (ss *SQLServer) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	// run migration
	query := string(migr[:])
	if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
		if msErr, ok := err.(mssql.Error); ok {
			message := fmt.Sprintf("migration failed: %s", msErr.Message)
			if msErr.ProcName != "" {
				message = fmt.Sprintf("%s (proc name %s)", msErr.Message, msErr.ProcName)
			}
			return database.Error{OrigErr: err, Err: message, Query: migr, Line: uint(msErr.LineNo)}
		}
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

// SetVersion for the current database
func (ss *SQLServer) SetVersion(version int, dirty bool) error {

	tx, err := ss.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `TRUNCATE TABLE "` + ss.config.MigrationsTable + `"`
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		var dirtyBit int
		if dirty {
			dirtyBit = 1
		}
		query = `INSERT INTO "` + ss.config.MigrationsTable + `" (version, dirty) VALUES (@p1, @p2)`
		if _, err := tx.Exec(query, version, dirtyBit); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

// Version of the current database state
func (ss *SQLServer) Version() (version int, dirty bool, err error) {
	query := `SELECT TOP 1 version, dirty FROM "` + ss.config.MigrationsTable + `"`
	err = ss.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		// FIXME: convert to MSSQL error
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

// Drop all tables from the database.
func (ss *SQLServer) Drop() error {

	// drop all referential integrity constraints
	query := `
	DECLARE @Sql NVARCHAR(500) DECLARE @Cursor CURSOR

	SET @Cursor = CURSOR FAST_FORWARD FOR
	SELECT DISTINCT sql = 'ALTER TABLE [' + tc2.TABLE_NAME + '] DROP [' + rc1.CONSTRAINT_NAME + ']'
	FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc1
	LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc2 ON tc2.CONSTRAINT_NAME =rc1.CONSTRAINT_NAME

	OPEN @Cursor FETCH NEXT FROM @Cursor INTO @Sql

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
	Exec sp_executesql @Sql
	FETCH NEXT FROM @Cursor INTO @Sql
	END

	CLOSE @Cursor DEALLOCATE @Cursor`

	if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// drop the tables
	query = `EXEC sp_MSforeachtable 'DROP TABLE ?'`
	if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (ss *SQLServer) ensureVersionTable() (err error) {
	if err = ss.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := ss.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := `IF NOT EXISTS
	(SELECT *
		 FROM sysobjects
		WHERE id = object_id(N'[dbo].[` + ss.config.MigrationsTable + `]')
			AND OBJECTPROPERTY(id, N'IsUserTable') = 1
	)
	CREATE TABLE ` + ss.config.MigrationsTable + ` ( version BIGINT PRIMARY KEY NOT NULL, dirty BIT NOT NULL );`

	if _, err = ss.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func getMSITokenProvider(resource string) (func() (string, error), error) {
	msi, err := adal.NewServicePrincipalTokenFromManagedIdentity(resource, nil)
	if err != nil {
		return nil, err
	}

	return func() (string, error) {
		err := msi.EnsureFresh()
		if err != nil {
			return "", err
		}
		token := msi.OAuthToken()
		return token, nil
	}, nil
}

// The sql server resource can change across clouds so get it
// dynamically based on the server uri.
// ex. <server name>.database.windows.net -> https://database.windows.net
func getAADResourceFromServerUri(purl *nurl.URL) string {
	return fmt.Sprintf("%s%s", "https://", strings.Join(strings.Split(purl.Hostname(), ".")[1:], "."))
}
