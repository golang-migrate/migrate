package immudb

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	nurl "net/url"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"go.uber.org/atomic"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
)

var _ database.Driver = (*Immudb)(nil) // explicit compile time type check

func init() {
	database.Register("immudb", &Immudb{})
}

var (
	DefaultMigrationsTable = "schema_migrations"
	DefaultLockKey         = "schema_migrations_lock"
)

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

const (
	KeyNotFoundError = "rpc error: code = Unknown desc = key not found"
	StateLocked      = "locked"
	StateNotLocked   = "not-locked"
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
	LockKey         string
}

type Immudb struct {
	client   immudb.ImmuClient
	isLocked atomic.Bool
	config   *Config
	dropped  bool
}

func WithInstance(instance immudb.ImmuClient, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if config == nil {
		return nil, ErrNilConfig
	}
	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}
	if len(config.LockKey) == 0 {
		config.LockKey = DefaultLockKey
	}

	if err := instance.HealthCheck(ctx); err != nil {
		return nil, err
	}

	ic := &Immudb{
		client: instance,
		config: config,
	}

	if err := ic.ensureVersionTable(); err != nil {
		return nil, err
	}

	return ic, nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Immudb type.
func (i *Immudb) ensureVersionTable() (err error) {
	if err = i.Lock(); err != nil {
		return err
	}
	defer func() {
		if e := i.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := "CREATE TABLE IF NOT EXISTS " + i.config.MigrationsTable + " (version INTEGER, dirty BOOLEAN, PRIMARY KEY version)"
	if _, err = i.client.SQLExec(context.Background(), query, nil); err != nil {
		return err
	}
	return nil
}

func (i *Immudb) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}
	host, aport, err := net.SplitHostPort(purl.Host)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(aport)
	if err != nil {
		return nil, err
	}
	password, _ := purl.User.Password()
	databaseName := strings.TrimPrefix(purl.Path, "/")
	migrationsTable := purl.Query().Get("x-migrations-table")
	lockKey := purl.Query().Get("x-advisory-lock-key")

	options := immudb.DefaultOptions().WithAddress(host).WithPort(port)
	client := immudb.NewClient().WithOptions(options)

	err = client.OpenSession(
		context.Background(),
		[]byte(purl.User.Username()),
		[]byte(password),
		databaseName,
	)
	if err != nil {
		return nil, err
	}

	ic, err := WithInstance(client, &Config{
		MigrationsTable: migrationsTable,
		DatabaseName:    databaseName,
		LockKey:         lockKey,
	})
	if err != nil {
		return nil, err
	}

	return ic, nil
}

func (i *Immudb) Close() error {
	return i.client.CloseSession(context.Background())
}

func (i *Immudb) Lock() error {
	return database.CasRestoreOnErr(&i.isLocked, false, true, database.ErrLocked, func() error {
		return i.setLockedState(StateLocked)
	})
}

func (i *Immudb) Unlock() error {
	if i.dropped {
		// Can't retrieve a lock from a dropped database
		i.isLocked.CAS(true, false)
		return nil
	}
	return database.CasRestoreOnErr(&i.isLocked, true, false, database.ErrNotLocked, func() error {
		return i.setLockedState(StateNotLocked)
	})
}

func (i *Immudb) setLockedState(state string) error {
	entry, err := i.client.Get(context.Background(), []byte(i.config.LockKey))
	if err != nil && err.Error() != KeyNotFoundError {
		return err
	}
	if entry != nil && string(entry.Value) == state {
		return &database.Error{Err: "unexpected state"}
	}

	var preconditions []*schema.Precondition
	if entry != nil {
		preconditions = append(preconditions, schema.PreconditionKeyNotModifiedAfterTX(
			[]byte(i.config.LockKey),
			entry.Tx,
		))
	}
	_, err = i.client.SetAll(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte(i.config.LockKey),
			Value: []byte(state),
		}},
		Preconditions: preconditions,
	})
	if err != nil {
		return err
	}
	return nil
}

func (i *Immudb) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr[:])

	if _, err := i.client.SQLExec(context.Background(), query, nil); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}
	return nil
}

func (i *Immudb) SetVersion(version int, dirty bool) error {
	query := "DELETE FROM " + i.config.MigrationsTable
	_, err := i.client.SQLExec(context.Background(), query, nil)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := "INSERT INTO " + i.config.MigrationsTable + " (version, dirty) VALUES (@version, @dirty)"
		params := map[string]interface{}{
			"version": version,
			"dirty":   dirty,
		}
		_, err = i.client.SQLExec(context.Background(), query, params)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	return nil
}

func (i *Immudb) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + i.config.MigrationsTable + " LIMIT 1"
	result, err := i.client.SQLQuery(context.Background(), query, nil, true)
	if err != nil {
		return database.NilVersion, false, err
	}
	for _, row := range result.Rows {
		if len(row.Values) != 2 {
			return database.NilVersion, false, database.Error{Err: "unexpected version row"}
		}
		return int(row.Values[0].GetN()), row.Values[1].GetB(), nil
	}
	return database.NilVersion, false, nil
}

func (i *Immudb) Drop() error {
	_, err := i.client.UnloadDatabase(context.Background(), &schema.UnloadDatabaseRequest{
		Database: i.config.DatabaseName,
	})
	if err != nil {
		return err
	}
	_, err = i.client.DeleteDatabase(context.Background(), &schema.DeleteDatabaseRequest{
		Database: i.config.DatabaseName,
	})
	if err != nil {
		return err
	}
	i.dropped = true
	return nil
}
