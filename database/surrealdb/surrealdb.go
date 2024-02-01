package surrealdb

import (
	"fmt"
	"io"
	nurl "net/url"
	"os"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"github.com/surrealdb/surrealdb.go"
)

func init() {
	database.Register("surreal", &SurrealDB{})
	database.Register("surrealdb", &SurrealDB{})
}

var DefaultMigrationsTable = "schema_migrations"
var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	MigrationsTable string
	Namespace       string
	DatabaseName    string
}

func (c *Config) GetVersionDocumentId() (docId string) {
	return fmt.Sprintf("%s:version", c.MigrationsTable)
}

func (c *Config) GetLockDocumentId() (docId string) {
	return fmt.Sprintf("%s:lock", c.MigrationsTable)
}

type SurrealDB struct {
	db     *surrealdb.DB
	config *Config
}

type DBInfo struct {
	DL map[string]string `json:"dl"`
	DT map[string]string `json:"dt"`
	FC map[string]string `json:"fc"`
	PA map[string]string `json:"pa"`
	SC map[string]string `json:"sc"`
	TB map[string]string `json:"tb"`
}

type VersionInfo struct {
	ID      string `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`
	Dirty   bool   `json:"dirty,omitempty"`
}

type LockDoc struct {
	ID        string `json:"id,omitempty"`
	Pid       int    `json:"pid,omitempty"`
	Hostname  string `json:"hostname,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

func WithInstance(instance *surrealdb.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if _, err := instance.Info(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	mx := &SurrealDB{
		db:     instance,
		config: config,
	}
	if err := mx.ensureVersionTable(); err != nil {
		return nil, err
	}
	return mx, nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the SurrealDB type.
func (m *SurrealDB) ensureVersionTable() (err error) {
	if err = m.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	if err != nil {
		return err
	}
	if _, _, err = m.Version(); err != nil {
		return err
	}
	return nil
}

func (m *SurrealDB) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	qv := purl.Query()

	migrationsTable := qv.Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	scheme := "wss"
	host := purl.Host
	path := strings.TrimPrefix(strings.TrimPrefix(purl.Path, "/rpc/"), "/")
	username := purl.User.Username()
	password, _ := purl.User.Password()

	if len(purl.Query().Get("sslmode")) > 0 {
		if purl.Query().Get("sslmode") == "disable" {
			scheme = "ws"
		}
	}

	split_path := strings.SplitN(path, "/", 2)
	namespace, database_name := split_path[0], split_path[1]

	if len(namespace) < 1 {
		return nil, fmt.Errorf("missing namespace in path: %s", path)
	} else if len(database_name) < 1 {
		return nil, fmt.Errorf("missing dataspace name in path: %s", path)
	} else if strings.Contains(namespace, "/") {
		return nil, fmt.Errorf("bad path: %s. Path should be in format '/namespace/database'", path)
	} else if strings.Contains(database_name, "/") {
		return nil, fmt.Errorf("bad path: %s. Path should be in format '/namespace/database'", path)
	}

	connUrl := fmt.Sprintf("%s://%s/rpc", scheme, host)

	db, err := surrealdb.New(connUrl)
	if err != nil {
		return nil, err
	}

	_, err = db.Signin(map[string]interface{}{
		"user": username,
		"pass": password,
	})
	if err != nil {
		return nil, err
	}

	_, err = db.Use(namespace, database_name)
	if err != nil {
		return nil, err
	}

	mx, err := WithInstance(db, &Config{
		Namespace:       namespace,
		DatabaseName:    database_name,
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return nil, err
	}

	return mx, nil
}

func (m *SurrealDB) Close() error {
	m.db.Close()
	return nil
}

func (m *SurrealDB) Drop() (err error) {
	query := `INFO FOR DB;`
	result, err := surrealdb.SmartUnmarshal[DBInfo](m.db.Query(query, map[string]interface{}{}))
	if err != nil {
		return err
	}

	for tableName := range result.TB {
		query := fmt.Sprintf(`REMOVE TABLE %s;`, tableName)
		_, err := m.db.Query(query, map[string]interface{}{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *SurrealDB) Lock() error {
	pid := os.Getpid()
	hostname, err := os.Hostname()
	if err != nil {
		hostname = fmt.Sprintf("Could not determine hostname. Error: %s", err.Error())
	}

	_, err = m.db.Create(m.config.MigrationsTable, LockDoc{
		ID:        "lock",
		Pid:       pid,
		Hostname:  hostname,
		CreatedAt: time.Now().Format(time.RFC3339),
	})

	return err
}

func (m *SurrealDB) Unlock() error {
	lock_doc_id := m.config.GetLockDocumentId()
	query := `BEGIN; LET $lock = SELECT * FROM $lock_doc_id; DELETE $lock; COMMIT;`

	// Delete will error if lock_doc_id does not exist because $lock ends up as NONE
	_, err := m.db.Query(query, map[string]interface{}{"lock_doc_id": lock_doc_id})
	return err
}

func (m *SurrealDB) Run(migration io.Reader) error {
	mig, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(mig[:])
	_, err = m.db.Query(query, map[string]interface{}{})
	return err
}

func (m *SurrealDB) SetVersion(version int, dirty bool) error {
	version_document_id := m.config.GetVersionDocumentId()
	params := map[string]interface{}{"version_document_id": version_document_id}
	query := `BEGIN; DELETE $version_document_id; `

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		params = map[string]interface{}{
			"version_document_id": version_document_id,
			"version":             version,
			"dirty":               dirty,
		}
		query += `CREATE $version_document_id CONTENT {
					version: $version,
					dirty: $dirty
				}; `
	}

	query += `COMMIT;`

	_, err := m.db.Query(query, params)
	if err != nil {
		return err
	}

	return nil
}

func (m *SurrealDB) Version() (version int, dirty bool, err error) {
	version_document_id := m.config.GetVersionDocumentId()

	query := fmt.Sprintf("SELECT * FROM %s;", version_document_id)
	versionInfo, err := surrealdb.SmartUnmarshal[[]VersionInfo](m.db.Query(query, map[string]interface{}{}))
	if err != nil {
		return database.NilVersion, false, err
	} else if len(versionInfo) == 0 {
		return database.NilVersion, false, nil
	}

	return versionInfo[0].Version, versionInfo[0].Dirty, nil
}
