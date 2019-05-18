package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"

	_ "github.com/denisenkom/go-mssqldb" // mssql support
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	db := MSSQL{}
	database.Register("mssql", &db)
	database.Register("sqlserver", &db)
}

// DefaultMigrationsTable is the name of the migrations table in the database
var DefaultMigrationsTable = "SchemaMigrations"

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

// Config for database
type Config struct {
	MigrationsTable string
	DatabaseName    string
	SchemaName      string
}

// MSSQL connection
type MSSQL struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked bool

	// Open and WithInstance need to garantuee that config is never nil
	config *Config
}

// WithInstance returns a database instance from an already create database connection
// TODO: WithInstance double check that docs are correct for this function
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	query := `SELECT DB_NAME()`
	var databaseName string
	if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(databaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	config.DatabaseName = databaseName

	query = `SELECT SCHEMA_NAME()`
	var schemaName string
	if err := instance.QueryRow(query).Scan(&schemaName); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(schemaName) == 0 {
		return nil, ErrNoSchema
	}

	config.SchemaName = schemaName

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(context.Background())

	if err != nil {
		return nil, err
	}

	ss := &MSSQL{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := ss.ensureVersionTable(); err != nil {
		return nil, err
	}

	return ss, nil
}

func dbConnectionString(host, port string) string {
	return fmt.Sprintf("postgres://postgres@%s:%s/postgres?sslmode=disable", host, port)
}

// Open a connection to the database
func (ss *MSSQL) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mssql", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

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
func (ss *MSSQL) Close() error {
	connErr := ss.conn.Close()
	dbErr := ss.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// Lock creates an advisory local on the database to prevent multiple migrations from running at the same time.
func (ss *MSSQL) Lock() error {
	if ss.isLocked {
		return database.ErrLocked
	}

	aid, err := database.GenerateAdvisoryLockId(ss.config.DatabaseName, ss.config.SchemaName)
	if err != nil {
		return err
	}

	// start a transaction
	query := "BEGIN TRANSACTION"
	if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Err: "get lock transaction", Query: []byte(query)}
	}

	// This will either obtain the lock immediately and return true,
	// or return false if the lock cannot be acquired immediately.
	// MS Docs: sp_getapplock: https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-2017
	query = `EXEC sp_getapplock @Resource = ?, @LockMode = 'Update'`
	if _, err := ss.conn.ExecContext(context.Background(), query, aid); err != nil {
		return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
	}

	ss.isLocked = true
	return nil
}

// Unlock froms the migration lock from the database
func (ss *MSSQL) Unlock() error {
	if !ss.isLocked {
		return nil
	}

	aid, err := database.GenerateAdvisoryLockId(ss.config.DatabaseName, ss.config.SchemaName)
	if err != nil {
		return err
	}

	// MS Docs: sp_releaseapplock: https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-releaseapplock-transact-sql?view=sql-server-2017
	query := `EXEC sp_releaseapplock @Resource = ?`
	if _, err := ss.conn.ExecContext(context.Background(), query, aid); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	ss.isLocked = false

	// end lock transaction
	query = "COMMIT"
	if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Err: "commit lock transaction", Query: []byte(query)}
	}

	return nil
}

// Run the migrations for the database
func (ss *MSSQL) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	// run migration
	query := string(migr[:])
	if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
		// // FIXME: check for mssql error here
		// if pgErr, ok := err.(*pq.Error); ok {
		// 	var line uint
		// 	var col uint
		// 	var lineColOK bool
		// 	if pgErr.Position != "" {
		// 		if pos, err := strconv.ParseUint(pgErr.Position, 10, 64); err == nil {
		// 			line, col, lineColOK = computeLineFromPos(query, int(pos))
		// 		}
		// 	}
		// 	message := fmt.Sprintf("migration failed: %s", pgErr.Message)
		// 	if lineColOK {
		// 		message = fmt.Sprintf("%s (column %d)", message, col)
		// 	}
		// 	if pgErr.Detail != "" {
		// 		message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
		// 	}
		// 	return database.Error{OrigErr: err, Err: message, Query: migr, Line: line}
		// }
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

// func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
// 	// replace crlf with lf
// 	s = strings.Replace(s, "\r\n", "\n", -1)
// 	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
// 	runes := []rune(s)
// 	if pos > len(runes) {
// 		return 0, 0, false
// 	}
// 	sel := runes[:pos]
// 	line = uint(runesCount(sel, newLine) + 1)
// 	col = uint(pos - 1 - runesLastIndex(sel, newLine))
// 	return line, col, true
// }

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

// SetVersion for the current database
func (ss *MSSQL) SetVersion(version int, dirty bool) error {

	tx := ss.db
	// tx, err := ss.conn.BeginTx(context.Background(), &sql.TxOptions{})
	// if err != nil {
	// 	return &database.Error{OrigErr: err, Err: "transaction start failed"}
	// }

	query := `TRUNCATE TABLE "` + ss.config.MigrationsTable + `"`
	if _, err := tx.Exec(query); err != nil {
		// tx.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		var dirtyBit int
		if dirty {
			dirtyBit = 1
		}
		query = `INSERT INTO "` + ss.config.MigrationsTable + `" (version, dirty) VALUES ($1, $2)`
		if _, err := tx.Exec(query, version, dirtyBit); err != nil {
			// tx.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	// if err := tx.Commit(); err != nil {
	// 	return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	// }

	return nil
}

// Version of the current database state
func (ss *MSSQL) Version() (version int, dirty bool, err error) {
	query := `SELECT TOP 1 version, dirty FROM "` + ss.config.MigrationsTable + `"`
	err = ss.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		// FIXME: convert to MSSQL error
		// if e, ok := err.(*pq.Error); ok {
		// 	if e.Code.Name() == "undefined_table" {
		// 		return database.NilVersion, false, nil
		// 	}
		// }
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

// Drop all tables from the database.
func (ss *MSSQL) Drop() error {

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

	if err := ss.ensureVersionTable(); err != nil {
		return err
	}

	return nil

	// // select all tables in current schema
	// query := `SELECT t.name FROM sys.tables AS t INNER JOIN sys.schemas AS s ON t.[schema_id] = s.[schema_id] WHERE s.name = N'dbo';`
	// tables, err := ss.conn.QueryContext(context.Background(), query)
	// if err != nil {
	// 	return &database.Error{OrigErr: err, Query: []byte(query)}
	// }
	// defer tables.Close()

	// // delete one table after another
	// tableNames := make([]string, 0)
	// for tables.Next() {
	// 	var tableName string
	// 	if err := tables.Scan(&tableName); err != nil {
	// 		return err
	// 	}
	// 	if len(tableName) > 0 {
	// 		tableNames = append(tableNames, tableName)
	// 	}
	// }

	// if len(tableNames) > 0 {
	// 	// delete one by one ...
	// 	for _, t := range tableNames {
	// 		query = `DROP TABLE IF EXISTS "` + t + `"`
	// 		if _, err := ss.conn.ExecContext(context.Background(), query); err != nil {
	// 			return &database.Error{OrigErr: err, Query: []byte(query)}
	// 		}
	// 	}

	// 	if err := ss.ensureVersionTable(); err != nil {
	// 		return err
	// 	}
	// }

	// return nil
}

func (ss *MSSQL) ensureVersionTable() (err error) {
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
