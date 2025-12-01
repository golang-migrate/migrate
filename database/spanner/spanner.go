package spanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	nurl "net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"cloud.google.com/go/spanner"
	sdb "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/spansql"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
)

func init() {
	db := Spanner{}
	database.Register("spanner", &db)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "SchemaMigrations"

type statementKind int

const (
	statementKindDDL statementKind = iota + 1
	statementKindDML
	statementKindPartitionedDML
)

// ErrNilConfig is returned when no configuration is provided.
var ErrNilConfig = errors.New("no config")

// ErrNoDatabaseName is returned when the database name is not specified in the configuration.
var ErrNoDatabaseName = errors.New("no database name")

// ErrNoSchema is returned when no schema is available.
var ErrNoSchema = errors.New("no schema")

// ErrDatabaseDirty is returned when the database has a dirty migration state.
var ErrDatabaseDirty = errors.New("database is dirty")

// ErrLockHeld is returned when attempting to acquire a lock that is already held.
var ErrLockHeld = errors.New("unable to obtain lock")

// ErrLockNotHeld is returned when attempting to release a lock that is not held.
var ErrLockNotHeld = errors.New("unable to release already released lock")

// ErrMixedStatements is returned when a migration file contains a mix of DDL,
// DML (INSERT), and partitioned DML (UPDATE or DELETE) statements.
var ErrMixedStatements = errors.New("DDL, DML (INSERT), and partitioned DML (UPDATE or DELETE) must not be combined in the same migration file")

// ErrEmptyMigration is returned when a migration file is empty or contains only whitespace.
var ErrEmptyMigration = errors.New("empty migration")

// ErrInvalidDMLStatementKind is returned when an unrecognized DML statement type is encountered.
var ErrInvalidDMLStatementKind = errors.New("invalid DML statement kind")

// Config holds the configuration for a Spanner database driver instance.
type Config struct {
	// MigrationsTable is the name of the table used to track migration versions.
	// If empty, DefaultMigrationsTable is used.
	MigrationsTable string

	// DatabaseName is the fully qualified Spanner database name
	// (e.g., "projects/{project}/instances/{instance}/databases/{database}").
	DatabaseName string

	// Deprecated: CleanStatements is no longer needed. Migration statements are
	// now automatically parsed to detect their type (DDL, DML, PartitionedDML)
	// and comments are stripped during parsing. This field is ignored.
	CleanStatements bool
}

// Spanner implements database.Driver for Google Cloud Spanner.
// It supports DDL statements (CREATE, ALTER, DROP), DML statements (INSERT),
// and partitioned DML statements (UPDATE, DELETE) in migration files.
type Spanner struct {
	db *DB

	config *Config

	lock atomic.Bool
}

// DB holds the Spanner client connections for both administrative operations
// (schema changes) and data operations (queries and mutations).
type DB struct {
	admin *sdb.DatabaseAdminClient
	data  *spanner.Client
}

// NewDB creates a new DB instance with the provided admin and data clients.
// The admin client is used for DDL operations, while the data client is used
// for DML operations and queries.
func NewDB(admin sdb.DatabaseAdminClient, data spanner.Client) *DB {
	return &DB{
		admin: &admin,
		data:  &data,
	}
}

// WithInstance creates a new Spanner driver using an existing DB instance.
// It validates the configuration and ensures the migrations version table exists.
func WithInstance(instance *DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	sx := &Spanner{
		db:     instance,
		config: config,
	}

	if err := sx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return sx, nil
}

// Open parses the connection URL and creates a new Spanner driver instance.
// The URL format is: spanner://projects/{project}/instances/{instance}/databases/{database}
//
// Supported query parameters:
//   - x-migrations-table: Custom name for the migrations tracking table
//   - x-clean-statements: Deprecated, this parameter is ignored. Statements are now always parsed.
func (s *Spanner) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	adminClient, err := sdb.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	dbname := strings.Replace(migrate.FilterCustomQuery(purl).String(), "spanner://", "", 1)
	dataClient, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		log.Fatal(err)
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	cleanQuery := purl.Query().Get("x-clean-statements")
	clean := false
	if cleanQuery != "" {
		clean, err = strconv.ParseBool(cleanQuery)
		if err != nil {
			return nil, err
		}
	}

	db := &DB{admin: adminClient, data: dataClient}
	return WithInstance(db, &Config{
		DatabaseName:    dbname,
		MigrationsTable: migrationsTable,
		CleanStatements: clean,
	})
}

// Close releases all resources held by the Spanner driver, including
// both the admin and data client connections.
func (s *Spanner) Close() error {
	s.db.data.Close()
	return s.db.admin.Close()
}

// Lock acquires an in-memory lock to prevent concurrent migrations.
// Note: This is a local lock only; Spanner DDL operations are inherently
// serialized by the service through UpdateDatabaseDdl request queuing.
func (s *Spanner) Lock() error {
	if swapped := s.lock.CompareAndSwap(false, true); swapped {
		return nil
	}
	return ErrLockHeld
}

// Unlock releases the in-memory lock acquired by Lock.
func (s *Spanner) Unlock() error {
	if swapped := s.lock.CompareAndSwap(true, false); swapped {
		return nil
	}
	return ErrLockNotHeld
}

// Run executes the migration statements read from the provided reader.
// It automatically detects the statement type and routes execution accordingly:
//   - DDL statements (CREATE, ALTER, DROP): Executed via UpdateDatabaseDdl
//   - DML statements (INSERT): Executed within a read-write transaction
//   - Partitioned DML (UPDATE, DELETE): Executed via PartitionedUpdate
//
// Migration files must not mix different statement types.
func (s *Spanner) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()

	stmts, kind, err := parseStatements(migr)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to parse migration", Query: migr}
	}

	switch kind {
	case statementKindDDL:
		return s.runDDL(ctx, stmts, migr)
	case statementKindDML:
		return s.runDML(ctx, stmts, migr)
	case statementKindPartitionedDML:
		return s.runPartitionedDML(ctx, stmts, migr)
	default:
		return &database.Error{OrigErr: ErrInvalidDMLStatementKind, Err: "unknown statement kind", Query: migr}
	}
}

func (s *Spanner) runDDL(ctx context.Context, stmts []string, migr []byte) error {
	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: stmts,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (s *Spanner) runDML(ctx context.Context, stmts []string, migr []byte) error {
	_, err := s.db.data.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			for _, stmt := range stmts {
				if _, err := txn.Update(ctx, spanner.Statement{SQL: stmt}); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}
	return nil
}

func (s *Spanner) runPartitionedDML(ctx context.Context, stmts []string, migr []byte) error {
	for _, stmt := range stmts {
		_, err := s.db.data.PartitionedUpdate(ctx, spanner.Statement{SQL: stmt})
		if err != nil {
			return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
		}
	}
	return nil
}

// SetVersion updates the migration version in the migrations tracking table.
// It atomically deletes all existing records and inserts the new version.
func (s *Spanner) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	_, err := s.db.data.ReadWriteTransaction(ctx,
		func(_ context.Context, txn *spanner.ReadWriteTransaction) error {
			m := []*spanner.Mutation{
				spanner.Delete(s.config.MigrationsTable, spanner.AllKeys()),
				spanner.Insert(s.config.MigrationsTable,
					[]string{"Version", "Dirty"},
					[]any{version, dirty},
				)}
			return txn.BufferWrite(m)
		})
	if err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}

// Version returns the current migration version and dirty state.
// If no version has been set, it returns database.NilVersion.
func (s *Spanner) Version() (version int, dirty bool, err error) {
	ctx := context.Background()

	stmt := spanner.Statement{
		SQL: `SELECT Version, Dirty FROM ` + s.config.MigrationsTable + ` LIMIT 1`,
	}
	iter := s.db.data.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	switch err {
	case iterator.Done:
		return database.NilVersion, false, nil
	case nil:
		var v int64
		if err = row.Columns(&v, &dirty); err != nil {
			return 0, false, &database.Error{OrigErr: err, Query: []byte(stmt.SQL)}
		}
		version = int(v)
	default:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(stmt.SQL)}
	}

	return version, dirty, nil
}

var nameMatcher = regexp.MustCompile(`(CREATE TABLE\s(\S+)\s)|(CREATE.+INDEX\s(\S+)\s)`)

// Drop removes all tables and indexes from the database by retrieving the
// current schema and generating DROP statements in reverse order.
// This reverse order ensures that dependent objects (like interleaved tables
// and indexes) are dropped before their parent tables.
func (s *Spanner) Drop() error {
	ctx := context.Background()
	res, err := s.db.admin.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{
		Database: s.config.DatabaseName,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "drop failed"}
	}
	if len(res.Statements) == 0 {
		return nil
	}

	stmts := make([]string, 0)
	for i := len(res.Statements) - 1; i >= 0; i-- {
		s := res.Statements[i]
		m := nameMatcher.FindSubmatch([]byte(s))

		if len(m) == 0 {
			continue
		} else if tbl := m[2]; len(tbl) > 0 {
			stmts = append(stmts, fmt.Sprintf(`DROP TABLE %s`, tbl))
		} else if idx := m[4]; len(idx) > 0 {
			stmts = append(stmts, fmt.Sprintf(`DROP INDEX %s`, idx))
		}
	}

	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: stmts,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(strings.Join(stmts, "; "))}
	}
	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(strings.Join(stmts, "; "))}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Spanner type.
func (s *Spanner) ensureVersionTable() (err error) {
	if err = s.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := s.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	ctx := context.Background()
	tbl := s.config.MigrationsTable
	iter := s.db.data.Single().Read(ctx, tbl, spanner.AllKeys(), []string{"Version"})
	if err := iter.Do(func(_ *spanner.Row) error { return nil }); err == nil {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
    Dirty    BOOL NOT NULL
	) PRIMARY KEY(Version)`, tbl)

	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: []string{stmt},
	})

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}
	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	return nil
}

// parseStatements attempts to parse migration content as DDL first, then as DML.
// Returns the parsed statements and their kind.
func parseStatements(migration []byte) ([]string, statementKind, error) {
	content := string(migration)
	if strings.TrimSpace(content) == "" {
		return nil, 0, ErrEmptyMigration
	}

	// Try parsing as DDL first
	ddl, ddlErr := spansql.ParseDDL("", content)
	if ddlErr == nil && len(ddl.List) > 0 {
		stmts := make([]string, 0, len(ddl.List))
		for _, stmt := range ddl.List {
			stmts = append(stmts, stmt.SQL())
		}
		return stmts, statementKindDDL, nil
	}

	// Try parsing as DML
	dml, dmlErr := spansql.ParseDML("", content)
	if dmlErr == nil && len(dml.List) > 0 {
		stmts := make([]string, 0, len(dml.List))
		for _, stmt := range dml.List {
			stmts = append(stmts, stmt.SQL())
		}
		kind, err := inspectDMLKind(dml.List)
		if err != nil {
			return nil, 0, err
		}
		return stmts, kind, nil
	}

	if ddlErr != nil {
		return nil, 0, ddlErr
	}
	return nil, 0, dmlErr
}

// inspectDMLKind determines if DML statements are regular DML (INSERT) or
// partitioned DML (UPDATE, DELETE). Mixed statement types are not allowed.
func inspectDMLKind(stmts []spansql.DMLStmt) (statementKind, error) {
	if len(stmts) == 0 {
		return statementKindDDL, nil
	}

	var hasDML, hasPartitionedDML bool
	for _, stmt := range stmts {
		switch stmt.(type) {
		case *spansql.Insert:
			hasDML = true
		case *spansql.Update, *spansql.Delete:
			hasPartitionedDML = true
		default:
			return 0, fmt.Errorf("%w: unknown DML statement type %T", ErrInvalidDMLStatementKind, stmt)
		}
	}

	switch {
	case hasDML && !hasPartitionedDML:
		return statementKindDML, nil
	case !hasDML && hasPartitionedDML:
		return statementKindPartitionedDML, nil
	default:
		return 0, ErrMixedStatements
	}
}
