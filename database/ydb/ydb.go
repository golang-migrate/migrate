package ydb

import (
	"context"
	"fmt"
	"io"
	"math"
	nurl "net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	db := YDB{}
	database.Register("ydb", &db)
}

const (
	DefaultMigrationsTable = "schema_migrations"
)

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	MigrationsTable string
}

type YDB struct {
	driver *ydb.Driver
	config *Config

	isLocked atomic.Bool
}

func WithInstance(instance *ydb.Driver, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}

	db := &YDB{
		driver: instance,
		config: config,
	}

	err := db.ensureVersionTable()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *YDB) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	query, err := nurl.ParseQuery(purl.RawQuery)
	if err != nil {
		return nil, err
	}

	purl.Scheme = "grpcs"
	if v := query.Get("x-insecure"); v != "" {
		insecure, err := strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}

		if insecure {
			purl.Scheme = "grpc"
		}
	}

	ctx := context.Background()
	if v := query.Get("x-connect-timeout"); v != "" {
		timeout, err := time.ParseDuration(v)
		if err != nil {
			return nil, err
		}

		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	purl = migrate.FilterCustomQuery(purl)
	driver, err := ydb.Open(ctx, purl.String(), withCredentials(query))
	if err != nil {
		return nil, err
	}

	return WithInstance(driver, &Config{
		MigrationsTable: query.Get("x-migrations-table"),
	})
}

func (db *YDB) Close() error {
	ctx := context.Background()
	return db.driver.Close(ctx)
}

func (db *YDB) Lock() error {
	if !db.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (db *YDB) Unlock() error {
	if !db.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (db *YDB) Run(migration io.Reader) error {
	query, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()

	res, err := db.driver.Scripting().Execute(ctx, string(query), nil)
	if err != nil {
		return err
	}

	return res.Close()
}

func (db *YDB) SetVersion(version int, dirty bool) error {
	if version < math.MinInt32 || math.MaxInt32 < version {
		return fmt.Errorf("out of range (%d)", version)
	}

	dquery := fmt.Sprintf(
		"DELETE FROM `%s`",
		db.config.MigrationsTable,
	)

	rquery := fmt.Sprintf(
		"REPLACE INTO `%s` (version, dirty) VALUES (%d, %t)",
		db.config.MigrationsTable, version, dirty,
	)

	exec := func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, dquery, nil)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dquery)}
		}

		err = res.Close()
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dquery)}
		}

		res, err = tx.Execute(ctx, rquery, nil)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(rquery)}
		}

		err = res.Close()
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(rquery)}
		}

		return nil
	}

	ctx := context.Background()
	return db.driver.Table().DoTx(ctx, exec)
}

func (db *YDB) Version() (version int, dirty bool, err error) {
	query := fmt.Sprintf(
		"SELECT version, dirty FROM `%s` LIMIT 1",
		db.config.MigrationsTable,
	)

	exec := func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, table.OnlineReadOnlyTxControl(), query, nil)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer res.Close()

		err = res.NextResultSetErr(ctx)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		version = database.NilVersion
		dirty = false

		if res.NextRow() {
			var v int32
			err = res.ScanWithDefaults(&v, &dirty)
			if err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}

			version = int(v)
		}

		err = res.Err()
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		return nil
	}

	ctx := context.Background()
	err = db.driver.Table().Do(ctx, exec)
	if err != nil {
		return 0, false, err
	}

	return version, dirty, nil
}

func (db *YDB) Drop() error {
	ctx := context.Background()

	var drop func(string) error
	drop = func(name string) error {
		dir, err := db.driver.Scheme().ListDirectory(ctx, name)
		if err != nil {
			return err
		}

		for _, child := range dir.Children {
			name := path.Join(name, child.Name)
			if strings.Contains(name, "/.sys") {
				continue
			}

			switch {
			case child.IsDirectory():
				err = drop(name)
				if err == nil {
					err = db.driver.Scheme().RemoveDirectory(ctx, name)
				}

			case child.IsTable():
				err = db.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
					return s.DropTable(ctx, name)
				})

			case child.IsColumnTable():
				err = db.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
					return s.DropTable(ctx, name)
				})

			case child.IsTopic():
				err = db.driver.Topic().Drop(ctx, name)
			}

			if err != nil {
				return err
			}
		}

		return nil
	}

	root := db.driver.Scheme().Database()
	return drop(root)
}

func (db *YDB) ensureVersionTable() (err error) {
	err = db.Lock()
	if err != nil {
		return err
	}

	defer func() {
		unerr := db.Unlock()
		if err != nil {
			err = multierror.Append(err, unerr)
		} else {
			err = unerr
		}
	}()

	query := fmt.Sprintf(
		"CREATE TABLE `%s` (version Int32 NOT NULL, dirty Bool, PRIMARY KEY (version))",
		db.config.MigrationsTable,
	)

	exec := func(ctx context.Context, s table.Session) error {
		err := s.ExecuteSchemeQuery(ctx, query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		return nil
	}

	ctx := context.Background()
	return db.driver.Table().Do(ctx, exec)
}
