package spanner

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	nurl "net/url"
	"regexp"
	"strings"

	"golang.org/x/net/context"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/mattes/migrate"
	mdb "github.com/mattes/migrate/database"

	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func init() {
	db := Spanner{}
	mdb.Register("spanner", &db)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "SchemaMigrations"

// Driver errors
var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

// Config used for a Spanner instance
type Config struct {
	MigrationsTable string
	DatabaseName    string
}

// Spanner implements database.Driver for Google Cloud Spanner
type Spanner struct {
	adminClient *database.DatabaseAdminClient
	dataClient  *spanner.Client

	config *Config
}

// Open implements database.Driver
func (s *Spanner) Open(url string) (mdb.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	dbname := strings.Replace(migrate.FilterCustomQuery(purl).String(), "spanner://", "", 1)

	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	dataClient, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		log.Fatal(err)
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	sx := &Spanner{
		adminClient: adminClient,
		dataClient:  dataClient,
		config: &Config{
			DatabaseName:    dbname,
			MigrationsTable: migrationsTable,
		},
	}

	if err := sx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return sx, nil
}

// Close implements database.Driver
func (s *Spanner) Close() error {
	s.dataClient.Close()
	return s.adminClient.Close()
}

// Lock implements database.Driver but doesn't do anything because Spanner only
// enqueues the UpdateDatabaseDdlRequest.
func (s *Spanner) Lock() error {
	return nil
}

// Unlock implements database.Driver but no action required, see Lock.
func (s *Spanner) Unlock() error {
	return nil
}

// Run implements database.Driver
func (s *Spanner) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	// run migration
	stmt := string(migr[:])
	ctx := context.Background()

	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: []string{stmt},
	})

	if err != nil {
		return &mdb.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	if err := op.Wait(ctx); err != nil {
		return &mdb.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

// SetVersion implements database.Driver
func (s *Spanner) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	_, err := s.dataClient.ReadWriteTransaction(ctx,
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
		return &mdb.Error{OrigErr: err}
	}

	return nil
}

// Version implements database.Driver
func (s *Spanner) Version() (version int, dirty bool, err error) {
	ctx := context.Background()

	stmt := spanner.Statement{
		SQL: `SELECT Version, Dirty FROM ` + s.config.MigrationsTable + ` LIMIT 1`,
	}
	iter := s.dataClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	switch err {
	case iterator.Done:
		return mdb.NilVersion, false, nil
	case nil:
		var v int64
		if err = row.Columns(&v, &dirty); err != nil {
			return 0, false, &mdb.Error{OrigErr: err, Query: []byte(stmt.SQL)}
		}
		version = int(v)
	default:
		return 0, false, &mdb.Error{OrigErr: err, Query: []byte(stmt.SQL)}
	}

	return version, dirty, nil
}

// Drop implements database.Driver. Retrieves the database schema first and
// creates statements to drop the indexes and tables accordingly.
// Note: The drop statements are created in reverse order to how they're
// provided in the schema. Assuming the schema describes how the database can
// be "build up", it seems logical to "unbuild" the database simply by going the
// opposite direction. More testing
func (s *Spanner) Drop() error {
	ctx := context.Background()
	res, err := s.adminClient.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{
		Database: s.config.DatabaseName,
	})
	if err != nil {
		return &mdb.Error{OrigErr: err, Err: "drop failed"}
	}
	if len(res.Statements) == 0 {
		return nil
	}

	r := regexp.MustCompile(`(CREATE TABLE\s(\S+)\s)|(CREATE.+INDEX\s(\S+)\s)`)
	stmts := make([]string, 0)
	for i := len(res.Statements) - 1; i >= 0; i-- {
		s := res.Statements[i]
		m := r.FindSubmatch([]byte(s))

		if len(m) == 0 {
			continue
		} else if tbl := m[2]; len(tbl) > 0 {
			stmts = append(stmts, fmt.Sprintf(`DROP TABLE %s`, tbl))
		} else if idx := m[4]; len(idx) > 0 {
			stmts = append(stmts, fmt.Sprintf(`DROP INDEX %s`, idx))
		}
	}

	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: stmts,
	})
	if err != nil {
		return &mdb.Error{OrigErr: err, Query: []byte(strings.Join(stmts, "; "))}
	}
	if err := op.Wait(ctx); err != nil {
		return &mdb.Error{OrigErr: err, Query: []byte(strings.Join(stmts, "; "))}
	}

	if err := p.ensureVersionTable(); err != nil {
		return err
	}

	return nil
}

func (s *Spanner) ensureVersionTable() error {
	ctx := context.Background()
	tbl := s.config.MigrationsTable
	iter := s.dataClient.Single().Read(ctx, tbl, spanner.AllKeys(), nil)
	if err := iter.Do(func(r *spanner.Row) error { return nil }); err == nil {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
    Dirty    BOOL NOT NULL
	) PRIMARY KEY(Version)`, tbl)

	op, err := s.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: []string{stmt},
	})

	if err != nil {
		return &mdb.Error{OrigErr: err, Query: []byte(stmt)}
	}
	if err := op.Wait(ctx); err != nil {
		return &mdb.Error{OrigErr: err, Query: []byte(stmt)}
	}

	return nil
}
