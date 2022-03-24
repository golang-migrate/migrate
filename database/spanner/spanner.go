package spanner

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	nurl "net/url"
	"regexp"
	"strconv"
	"strings"

	"context"

	"cloud.google.com/go/spanner"
	sdb "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/spansql"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"

	"github.com/hashicorp/go-multierror"
	uatomic "go.uber.org/atomic"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func init() {
	db := Spanner{}
	database.Register("spanner", &db)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "SchemaMigrations"

const (
	unlockedVal = 0
	lockedVal   = 1
)

// Driver errors
var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")
	ErrDatabaseDirty  = errors.New("database is dirty")
	ErrLockHeld       = errors.New("unable to obtain lock")
	ErrLockNotHeld    = errors.New("unable to release already released lock")
)

// Config used for a Spanner instance
type Config struct {
	MigrationsTable string
	DatabaseName    string
	// Whether to parse the migration DDL with spansql before
	// running them towards Spanner.
	// Parsing outputs clean DDL statements such as reformatted
	// and void of comments.
	CleanStatements bool
}

// Spanner implements database.Driver for Google Cloud Spanner
type Spanner struct {
	db *DB

	config *Config

	lock *uatomic.Uint32
}

type DB struct {
	admin *sdb.DatabaseAdminClient
	data  *spanner.Client
}

func NewDB(admin sdb.DatabaseAdminClient, data spanner.Client) *DB {
	return &DB{
		admin: &admin,
		data:  &data,
	}
}

// WithInstance implements database.Driver
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
		lock:   uatomic.NewUint32(unlockedVal),
	}

	if err := sx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return sx, nil
}

// Open implements database.Driver
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

// Close implements database.Driver
func (s *Spanner) Close() error {
	s.db.data.Close()
	return s.db.admin.Close()
}

// Lock implements database.Driver but doesn't do anything because Spanner only
// enqueues the UpdateDatabaseDdlRequest.
func (s *Spanner) Lock() error {
	if swapped := s.lock.CAS(unlockedVal, lockedVal); swapped {
		return nil
	}
	return ErrLockHeld
}

// Unlock implements database.Driver but no action required, see Lock.
func (s *Spanner) Unlock() error {
	if swapped := s.lock.CAS(lockedVal, unlockedVal); swapped {
		return nil
	}
	return ErrLockNotHeld
}

// Run implements database.Driver
func (s *Spanner) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	stmts := []string{string(migr)}
	if s.config.CleanStatements {
		stmts, err = cleanStatements(migr)
		if err != nil {
			return err
		}
	}

	ctx := context.Background()
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

// SetVersion implements database.Driver
func (s *Spanner) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	_, err := s.db.data.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			m := []*spanner.Mutation{
				spanner.Delete(s.config.MigrationsTable, spanner.AllKeys()),
				spanner.Insert(s.config.MigrationsTable,
					[]string{"Version", "Dirty"},
					[]interface{}{version, dirty},
				)}
			return txn.BufferWrite(m)
		})
	if err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}

// Version implements database.Driver
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

// Drop implements database.Driver. Retrieves the database schema first and
// creates statements to drop the indexes and tables accordingly.
// Note: The drop statements are created in reverse order to how they're
// provided in the schema. Assuming the schema describes how the database can
// be "build up", it seems logical to "unbuild" the database simply by going the
// opposite direction. More testing
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
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()
	tbl := s.config.MigrationsTable
	iter := s.db.data.Single().Read(ctx, tbl, spanner.AllKeys(), []string{"Version"})
	if err := iter.Do(func(r *spanner.Row) error { return nil }); err == nil {
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

func cleanStatements(migration []byte) ([]string, error) {
	// The Spanner GCP backend does not yet support comments for the UpdateDatabaseDdl RPC
	// (see https://issuetracker.google.com/issues/159730604) we use
	// spansql to parse the DDL and output valid stamements without comments
	ddl, err := spansql.ParseDDL("", string(migration))
	if err != nil {
		return nil, err
	}
	stmts := make([]string, 0, len(ddl.List))
	for _, stmt := range ddl.List {
		stmts = append(stmts, stmt.SQL())
	}
	return stmts, nil
}
