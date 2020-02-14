package oracle

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	multierror "github.com/hashicorp/go-multierror"
	_ "github.com/mattn/go-oci8"
)

func init() {
	db := Oracle{}
	database.Register("oracle", &db)
	database.Register("oci8", &db)
}

const (
	defaultMigrationsTable         = "SCHEMA_MIGRATIONS"
	defaultStatementSeparator      = ";"
	plsqlDefaultStatementSeparator = "---"
	plsqlStatementEndToken         = "END;"
)

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

type OraErr interface {
	Code() int
	Error() string
	Message() string
}

type Config struct {
	MigrationsTable         string
	PLSQLStatementSeparator string

	databaseName string
}

type Oracle struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	query := `SELECT SYS_CONTEXT('USERENV','DB_NAME') FROM DUAL`
	var dbName string
	if err := instance.QueryRow(query).Scan(&dbName); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if dbName == "" {
		return nil, ErrNoDatabaseName
	}

	config.databaseName = dbName

	if config.MigrationsTable == "" {
		config.MigrationsTable = defaultMigrationsTable
	}

	if config.PLSQLStatementSeparator == "" {
		config.PLSQLStatementSeparator = plsqlDefaultStatementSeparator
	}

	conn, err := instance.Conn(context.Background())

	if err != nil {
		return nil, err
	}

	ora := &Oracle{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := ora.ensureVersionTable(); err != nil {
		return nil, err
	}

	return ora, nil
}

func (ora *Oracle) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}
	if purl.Scheme != "oracle" {
		return nil, errors.New("invalid schema expected oracle, got: " + purl.Scheme)
	}

	db, err := sql.Open("oci8", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsTable := strings.ToUpper(purl.Query().Get("x-migrations-table"))
	statementSeparator := purl.Query().Get("x-statement-separator")

	oraInst, err := WithInstance(db, &Config{
		databaseName:            purl.Path,
		MigrationsTable:         migrationsTable,
		PLSQLStatementSeparator: statementSeparator,
	})

	if err != nil {
		return nil, err
	}

	return oraInst, nil
}

func (ora *Oracle) Close() error {
	connErr := ora.conn.Close()
	dbErr := ora.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (ora *Oracle) Lock() error {
	if ora.isLocked {
		return database.ErrLocked
	}

	// https://docs.oracle.com/cd/B28359_01/appdev.111/b28419/d_lock.htm#ARPLS021
	query := `
declare
    v_lockhandle varchar2(200);
    v_result     number;
begin

    dbms_lock.allocate_unique('control_lock', v_lockhandle);

    v_result := dbms_lock.request(v_lockhandle, dbms_lock.x_mode);

    if v_result <> 0 then
        dbms_output.put_line(
                case
                    when v_result=1 then 'Timeout'
                    when v_result=2 then 'Deadlock'
                    when v_result=3 then 'Parameter Error'
                    when v_result=4 then 'Already owned'
                    when v_result=5 then 'Illegal Lock Handle'
                    end);
    end if;

end;
`
	if _, err := ora.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
	}

	ora.isLocked = true
	return nil
}

func (ora *Oracle) Unlock() error {
	if !ora.isLocked {
		return nil
	}

	query := `
declare
  v_lockhandle varchar2(200);
  v_result     number;
begin

  dbms_lock.allocate_unique('control_lock', v_lockhandle);

  v_result := dbms_lock.release(v_lockhandle);

  if v_result <> 0 then 
    dbms_output.put_line(
           case 
              when v_result=1 then 'Timeout'
              when v_result=2 then 'Deadlock'
              when v_result=3 then 'Parameter Error'
              when v_result=4 then 'Already owned'
              when v_result=5 then 'Illegal Lock Handle'
            end);
  end if;

end;
`
	if _, err := ora.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	ora.isLocked = false
	return nil
}

func (ora *Oracle) Run(migration io.Reader) error {
	queries, err := parseStatements(migration, ora.config.PLSQLStatementSeparator)
	if err != nil {
		return err
	}
	for _, query := range queries {
		if _, err := ora.conn.ExecContext(context.Background(), query); err != nil {
			if sqlError, ok := err.(OraErr); ok {
				return database.Error{OrigErr: err, Err: sqlError.Message(), Query: []byte(query)}
			}
			return database.Error{OrigErr: err, Err: "migration failed", Query: []byte(query)}
		}
	}

	return nil
}

func (ora *Oracle) SetVersion(version int, dirty bool) error {
	tx, err := ora.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "TRUNCATE TABLE " + ora.config.MigrationsTable
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		query = `INSERT INTO ` + ora.config.MigrationsTable + ` (VERSION, DIRTY) VALUES (:1, :2)`
		if _, err := tx.Exec(query, version, b2i(dirty)); err != nil {
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

func (ora *Oracle) Version() (version int, dirty bool, err error) {
	query := "SELECT VERSION, DIRTY FROM " + ora.config.MigrationsTable + " WHERE ROWNUM = 1 ORDER BY VERSION desc"
	err = ora.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if _, ok := err.(OraErr); ok {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (ora *Oracle) Drop() (err error) {
	// select all tables in current schema
	query := fmt.Sprintf(`SELECT TABLE_NAME FROM USER_TABLES`)
	tables, err := ora.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}

	query = `
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE %s';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
`
	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			if _, err := ora.conn.ExecContext(context.Background(), fmt.Sprintf(query, t)); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Postgres type.
func (ora *Oracle) ensureVersionTable() (err error) {
	if err = ora.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := ora.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := `
declare
v_sql LONG;
begin

v_sql:='create table %s
  (
  VERSION NUMBER(20) NOT NULL PRIMARY KEY,
  DIRTY NUMBER(1) NOT NULL
  )';
execute immediate v_sql;

EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -955 THEN
        NULL; -- suppresses ORA-00955 exception
      ELSE
         RAISE;
      END IF;
END;
`
	if _, err = ora.conn.ExecContext(context.Background(), fmt.Sprintf(query, ora.config.MigrationsTable)); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func parseStatements(rd io.Reader, plsqlStatementSeparator string) ([]string, error) {
	migr, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	plsqlEnabled := false
	if strings.Contains(string(migr), plsqlStatementEndToken) {
		plsqlEnabled = true
	}
	var queries []string
	var buf bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewBuffer(migr))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if plsqlEnabled && line == plsqlStatementSeparator {
			query := buf.String()
			if query != "" {
				queries = append(queries, query)
			}
			buf.Reset()
		}
		// ignore comment
		if strings.HasPrefix(line, "--") {
			continue
		}
		if _, err := buf.WriteString(line + "\n"); err != nil {
			return nil, err
		}
	}
	if plsqlEnabled {
		query := buf.String()
		if query != "" {
			queries = append(queries, query)
		}
	} else {
		queries = strings.Split(buf.String(), defaultStatementSeparator)
	}
	var results []string
	sLen := len(plsqlStatementEndToken)
	for _, query := range queries {
		query = strings.TrimSpace(query)
		query = strings.TrimPrefix(query, "\n")
		query = strings.TrimSuffix(query, "\n")
		if len(query) > sLen && strings.ToUpper(query[len(query)-sLen:]) != plsqlStatementEndToken {
			query = strings.TrimSuffix(query, ";")
		}
		if query == "" {
			continue
		}
		results = append(results, query)
	}
	return results, nil
}

func b2i(b bool) int8 {
	if b {
		return 1
	}
	return 0
}
