package gbase8s

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	_ "gitee.com/GBase8s/go-gci"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"go.uber.org/atomic"
)

func init() {
	database.Register("gbase8s", &Gbase8s{})
}

var (
	_                      database.Driver = (*Gbase8s)(nil)
	DefaultMigrationsTable                 = "schema_migrations"
	DefaultLockTable                       = "schema_lock"
)

var (
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNilConfig      = fmt.Errorf("no config")
)

type Config struct {
	MigrationsTable  string
	DatabaseName     string
	LockTable        string
	ForceLock        bool
	StatementTimeout time.Duration
}

type Gbase8s struct {
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool
	config   *Config
}

func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*Gbase8s, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	gx := &Gbase8s{
		conn:   conn,
		db:     nil,
		config: config,
	}

	if config.DatabaseName == "" {
		query := `SELECT DBINFO('dbname') FROM systables WHERE tabid = 1`
		var databaseName sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName.String) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName.String
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if len(config.LockTable) == 0 {
		config.LockTable = DefaultLockTable
	}

	if err := gx.ensureLockTable(); err != nil {
		return nil, err
	}

	if err := gx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return gx, nil
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	gx, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}

	gx.db = instance

	return gx, nil
}

func (g *Gbase8s) Open(dns string) (database.Driver, error) {
	gurl, err := url.Parse(dns)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("gbase8s", gurl.String())
	if err != nil {
		return nil, err
	}

	// migrationsTable := gurl.Query().Get("x-migrations-table")
	// if len(migrationsTable) == 0 {
	// 	migrationsTable = DefaultMigrationsTable
	// }

	// lockTable := gurl.Query().Get("x-lock-table")
	// if len(lockTable) == 0 {
	// 	lockTable = DefaultLockTable
	// }

	// forceLockQuery := gurl.Query().Get("x-force-lock")
	// forceLock, err := strconv.ParseBool(forceLockQuery)
	// if err != nil {
	// 	forceLock = false
	// }

	// statementTimeoutQuery := gurl.Query().Get("x-statement-timeout")
	// statementTimeout, err := strconv.Atoi(statementTimeoutQuery)
	// if err != nil {
	// 	statementTimeout = 0
	// }

	migrationsTable := DefaultMigrationsTable
	lockTable := DefaultLockTable
	forceLock := false
	statementTimeout := 0

	gx, err := WithInstance(db, &Config{
		DatabaseName:     gurl.Path,
		MigrationsTable:  migrationsTable,
		LockTable:        lockTable,
		ForceLock:        forceLock,
		StatementTimeout: time.Duration(statementTimeout) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}

	return gx, nil
}

func (g *Gbase8s) Close() error {
	connErr := g.conn.Close()
	var dbErr error
	if g.db != nil {
		dbErr = g.db.Close()
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (g *Gbase8s) Lock() error {
	return database.CasRestoreOnErr(&g.isLocked, false, true, database.ErrLocked, func() error {
		tx, err := g.conn.BeginTx(context.Background(), nil)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}()

		aid, err := database.GenerateAdvisoryLockId(g.config.DatabaseName)
		if err != nil {
			return err
		}

		query := "SELECT lock_id FROM " + g.config.LockTable + " WHERE lock_id = ?"
		rows, err := tx.QueryContext(context.Background(), query, aid)
		if err != nil {
			return database.Error{OrigErr: err, Err: "failed to fetch migration lock", Query: []byte(query)}
		}
		defer rows.Close()

		if rows.Next() {
			if !g.config.ForceLock {
				return database.ErrLocked
			}
			query = "DELETE FROM " + g.config.LockTable + " WHERE lock_id = ?"
			if _, err := tx.ExecContext(context.Background(), query, aid); err != nil {
				return database.Error{OrigErr: err, Err: "failed to force release lock", Query: []byte(query)}
			}
		}

		query = "INSERT INTO " + g.config.LockTable + " (lock_id) VALUES (?)"
		if _, err := tx.ExecContext(context.Background(), query, aid); err != nil {
			return database.Error{OrigErr: err, Err: "failed to set migration lock", Query: []byte(query)}
		}

		return nil
	})
}

func (g *Gbase8s) Unlock() error {
	return database.CasRestoreOnErr(&g.isLocked, true, false, database.ErrNotLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(g.config.DatabaseName)
		if err != nil {
			return err
		}

		query := "DELETE FROM " + g.config.LockTable + " WHERE lock_id = ?"
		if _, err := g.conn.ExecContext(context.Background(), query, aid); err != nil {
			if strings.Contains(err.Error(), "ERROR: -206:  42000") {
				// ERROR: -206:  42000 is "Table Not Exists Error" in Gbase8s
				// when the lock table is fully removed;  This is fine, and is a valid "unlocked" state for the schema
				return nil
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		return nil
	})
}

func (g *Gbase8s) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if g.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.config.StatementTimeout)
		defer cancel()
	}

	query := string(migr[:])
	if _, err := g.conn.ExecContext(ctx, query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (g *Gbase8s) SetVersion(version int, dirty bool) error {
	tx, err := g.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + g.config.MigrationsTable
	if _, err := tx.ExecContext(context.Background(), query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 || (version == database.NilVersion && dirty) {
		query := "INSERT INTO " + g.config.MigrationsTable + "(version, dirty) VALUES (?, ?)"
		if _, err := tx.ExecContext(context.Background(), query, version, dirty); err != nil {
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

func (g *Gbase8s) Version() (version int, dirty bool, err error) {
	query := "SELECT FIRST 1 version, dirty FROM " + g.config.MigrationsTable
	err = g.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}

func (g *Gbase8s) Drop() (err error) {
	query := "SELECT tabname FROM systables WHERE tabid > 1000 AND tabtype = 'T'"
	rows, err := g.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		tables = append(tables, table)
	}

	for _, tbl := range tables {
		if _, err := g.conn.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl)); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gbase8s) ensureVersionTable() (err error) {
	if err = g.Lock(); err != nil {
		return err
	}
	defer func() {
		if unlockErr := g.Unlock(); unlockErr != nil {
			err = multierror.Append(err, unlockErr)
		}
	}()

	query := `CREATE TABLE IF NOT EXISTS "` + g.config.MigrationsTable + `" (version INT NOT NULL PRIMARY KEY, dirty SMALLINT NOT NULL)`
	if _, err = g.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (g *Gbase8s) ensureLockTable() error {
	query := `CREATE TABLE IF NOT EXISTS "` + g.config.LockTable + `" (lock_id INT NOT NULL PRIMARY KEY)`
	if _, err := g.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}
