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

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

const (
	migrationsTableQueryParam = "x-migrations-table"
	useGRPCSQueryParam        = "x-use-grpcs"

	defaultMigrationsTable = "schema_migrations"
)

var _ database.Driver = (*YDB)(nil)

func init() {
	database.Register("ydb", &YDB{})
}

const (
	createVersionTableQueryTemplate = `
	CREATE TABLE %s (
		version Int32,
		dirty Bool,
		applied_at Timestamp,
		PRIMARY KEY (version)
	);
	`

	deleteVersionsQueryTemplate = `
	DELETE FROM %s;`

	setVersionQueryTemplate = `
	DECLARE $version AS Int32;
	DECLARE $dirty AS Bool;
	DECLARE $applied_at AS Timestamp;
	UPSERT INTO %s (version, dirty, applied_at) 
	VALUES ($version, $dirty, $applied_at);`

	getCurrentVersionQueryTemplate = `
	SELECT version, dirty FROM %s 
	ORDER BY version DESC LIMIT 1;`

	dropTablesQueryTemplate = "DROP TABLE `%s`;"

	getAllTablesQueryTemplate = "SELECT Path FROM `%s.sys/partition_stats` WHERE Path NOT LIKE '%%.sys%%'"
)

type Config struct {
	MigrationsTable string
	Path            string
}

func WithInstance(db *sql.DB, config Config) (database.Driver, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = defaultMigrationsTable
	}

	if config.Path != "" && !strings.HasSuffix(config.Path, "/") {
		config.Path += "/"
	}

	ydbDriver := &YDB{
		db:     db,
		config: config,
	}

	err := ydbDriver.createMigrationsTable(context.Background())
	if err != nil {
		return nil, database.Error{
			OrigErr: err,
			Err:     "failed to create migrations table",
		}
	}

	return ydbDriver, nil
}

type YDB struct {
	db     *sql.DB
	locked atomic.Bool
	config Config
}

func (y *YDB) tableWithPrefix(table string) string {
	if y.config.Path == "" {
		return table
	}

	return fmt.Sprintf("`%s%s`", y.config.Path, table)
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
	customUrl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	connUrl := migrate.FilterCustomQuery(customUrl)
	if customUrl.Query().Get(useGRPCSQueryParam) != "" {
		connUrl.Scheme = "grpcs"
	} else {
		connUrl.Scheme = "grpc"
	}

	nativeDriver, err := ydb.Open(context.Background(), connUrl.String())
	if err != nil {
		return nil, err
	}

	connector, err := ydb.Connector(nativeDriver)
	if err != nil {
		return nil, err
	}

	migrationsTable := customUrl.Query().Get(migrationsTableQueryParam)

	if migrationsTable == "" {
		migrationsTable = defaultMigrationsTable
	}

	databaseName := nativeDriver.Name()

	if databaseName != "" {
		databaseName += "/"
	}

	ydbDriver := &YDB{
		db: sql.OpenDB(connector),
		config: Config{
			MigrationsTable: migrationsTable,
			Path:            databaseName,
		},
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

	createTableQuery := fmt.Sprintf(createVersionTableQueryTemplate, y.tableWithPrefix(y.config.MigrationsTable))
	if _, err := y.db.ExecContext(ctx, createTableQuery); err != nil {
		return database.Error{
			OrigErr: err,
			Query:   []byte(createTableQuery),
		}
	}

	return nil
}

func (y *YDB) Close() error {
	return y.db.Close()
}

func (y *YDB) Drop() (err error) {
	tablesQuery := fmt.Sprintf(
		getAllTablesQueryTemplate,
		y.config.Path,
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

	_, err = y.db.ExecContext(ydb.WithQueryMode(context.Background(), ydb.ScriptingQueryMode), string(data))
	if err != nil {
		return database.Error{
			OrigErr: err,
			Err:     "migration failed",
			Query:   data,
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

	ctx := ydb.WithQueryMode(context.Background(), ydb.DataQueryMode)

	deleteVersions := fmt.Sprintf(deleteVersionsQueryTemplate, y.tableWithPrefix(y.config.MigrationsTable))
	if _, err = tx.ExecContext(ctx, deleteVersions); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}

		return database.Error{
			OrigErr: err,
			Err:     "failed to delete versions",
			Query:   []byte(deleteVersions),
		}
	}

	versionQuery := fmt.Sprintf(setVersionQueryTemplate, y.tableWithPrefix(y.config.MigrationsTable))
	_, err = tx.ExecContext(
		ctx,
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
			Query:   []byte(versionQuery),
		}
	}

	return tx.Commit()
}

func (y *YDB) Version() (version int, dirty bool, err error) {
	versionQuery := fmt.Sprintf(getCurrentVersionQueryTemplate, y.tableWithPrefix(y.config.MigrationsTable))

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
