package ydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4/database"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

const (
	migrationsTableQueryParam = "x-migrations-table"
	defaultMigrationsTable    = "schema_migrations"
)

var _ database.Driver = (*YDB)(nil)

func init() {
	database.Register("ydb", &YDB{})
}

type YDB struct {
	db              *sql.DB
	locked          atomic.Bool
	migrationsTable string
	prefix          string
}

func (y *YDB) tableWithPrefix(table string) string {
	return fmt.Sprintf("`%s/%s`", y.prefix, table)
}

func (y *YDB) Lock() error {
	if !y.locked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	return nil
}
func (y *YDB) Unlock() error {
	if !y.locked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	return nil
}

func (y *YDB) Open(dsn string) (database.Driver, error) {
	nativeDriver, err := ydb.Open(context.Background(), dsn)
	if err != nil {
		return nil, err
	}

	connector, err := ydb.Connector(nativeDriver)
	if err != nil {
		return nil, err
	}

	connUrl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	migrationsTable := connUrl.Query().Get(migrationsTableQueryParam)

	if migrationsTable == "" {
		migrationsTable = defaultMigrationsTable
	}

	ydbDriver := &YDB{
		db:              sql.OpenDB(connector),
		migrationsTable: migrationsTable,
		prefix:          nativeDriver.Name(),
	}

	err = ydbDriver.createMigrationsTable(context.Background())
	if err != nil {
		return nil, database.Error{
			OrigErr: err,
			Err:     "failed to create migrations table",
		}
	}

	return ydbDriver, nil
}

func (y *YDB) createMigrationsTable(ctx context.Context) (err error) {
	if err = y.Lock(); err != nil {
		return err
	}

	defer func() {
		if ierr := y.Unlock(); ierr != nil {
			err = errors.Join(err, ierr)
		}
	}()

	ctx = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)

	if _, err := y.db.ExecContext(ctx, fmt.Sprintf(createVersionTableQueryTemplate, y.tableWithPrefix(y.migrationsTable))); err != nil {
		return err
	}

	return nil
}

func (y *YDB) Close() error {
	return y.db.Close()
}

func (y *YDB) Drop() (err error) {
	tablesQuery := fmt.Sprintf(
		"SELECT Path FROM `%s/.sys/partition_stats` WHERE Path NOT LIKE '%%/.sys%%'",
		y.prefix,
	)

	rows, err := y.db.QueryContext(ydb.WithQueryMode(context.Background(), ydb.ScanQueryMode), tablesQuery)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tablesQuery)}
	}
	defer func() {
		if ierr := rows.Close(); ierr != nil {
			err = errors.Join(err, ierr)
		}
	}()

	if !rows.NextResultSet() {
		return nil
	}

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(tablesQuery)}
		}

		query := fmt.Sprintf(dropTablesQueryTemplate, table)

		if _, err = y.db.ExecContext(ydb.WithQueryMode(context.Background(), ydb.SchemeQueryMode), query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err = rows.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tablesQuery)}
	}

	return nil
}

func (y *YDB) Run(migration io.Reader) error {
	data, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	statements, err := skipComments(string(data))
	if err != nil {
		return database.Error{
			OrigErr: err,
			Err:     "failed to skip comments",
		}
	}

	currentMode := notSetMode

	for _, statement := range strings.Split(statements, ";") {
		statement = strings.TrimSpace(statement)

		if statement == "" {
			continue
		}

		mode := detectQueryMode(statement)

		if currentMode == notSetMode {
			currentMode = mode
		} else if currentMode != mode {
			return database.Error{
				Err:   "mixed query modes in one migration",
				Query: []byte(statements),
			}
		}
	}

	ctx := context.Background()

	switch currentMode {
	case ddlMode:
		ctx = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
	case dmlMode:
		ctx = ydb.WithQueryMode(ctx, ydb.DataQueryMode)
	}

	_, err = y.db.ExecContext(ctx, statements)
	if err != nil {
		return database.Error{
			OrigErr: err,
			Err:     "migration failed",
			Query:   []byte(statements),
		}
	}

	return nil
}

func (y *YDB) SetVersion(version int, dirty bool) error {
	tx, err := y.db.BeginTx(context.Background(), &sql.TxOptions{
		ReadOnly:  false,
		Isolation: sql.LevelDefault,
	})
	if err != nil {
		return err
	}

	deleteVersions := fmt.Sprintf(deleteVersionsQueryTemplate, y.tableWithPrefix(y.migrationsTable))

	if _, err = tx.Exec(deleteVersions); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}

		return database.Error{
			OrigErr: err,
			Err:     "failed to delete versions",
			Query:   []byte(deleteVersions),
		}
	}

	versionQuery := fmt.Sprintf(setVersionQueryTemplate, y.tableWithPrefix(y.migrationsTable))

	_, err = tx.Exec(
		versionQuery,
		sql.Named("version", version),
		sql.Named("dirty", dirty),
		sql.Named("applied_at", time.Now()),
	)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}

		return database.Error{
			OrigErr: err,
			Err:     "failed to set version",
		}
	}

	return tx.Commit()
}

func (y *YDB) Version() (version int, dirty bool, err error) {
	versionQuery := fmt.Sprintf(getVersionQueryTemplate, y.tableWithPrefix(y.migrationsTable))

	row, err := y.db.QueryContext(ydb.WithQueryMode(context.Background(), ydb.ScanQueryMode), versionQuery)
	if err != nil {
		return 0, false, database.Error{
			OrigErr: err,
			Err:     "failed to get version",
			Query:   []byte(versionQuery),
		}
	}
	if !row.NextResultSet() || !row.Next() {
		return database.NilVersion, false, nil
	}

	if err = row.Scan(&version, &dirty); err != nil {
		return 0, false, &database.Error{
			OrigErr: err,
			Err:     "failed to scan version",
			Query:   []byte(versionQuery),
		}
	}

	return version, dirty, err
}
