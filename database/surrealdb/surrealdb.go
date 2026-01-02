package surrealdb

import (
	"context"
	"fmt"
	"io"
	nurl "net/url"
	"os"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
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

type VersionInfo struct {
	ID      string `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`
	Dirty   bool   `json:"dirty,omitempty"`
}

func WithInstance(instance *surrealdb.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if instance == nil {
		return nil, fmt.Errorf("instance is nil")
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
	ctx := context.Background()
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

	db, err := surrealdb.FromEndpointURLString(ctx, connUrl)
	if err != nil {
		return nil, err
	}

	_, err = db.SignIn(ctx, &surrealdb.Auth{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	err = db.Use(ctx, namespace, database_name)
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
	return m.db.Close(context.Background())
}

func (m *SurrealDB) Drop() (err error) {
	query := `INFO FOR DB;`
	results, err := surrealdb.Query[map[string]any](context.Background(), m.db, query, map[string]any{})
	if err != nil {
		return err
	}

	if results == nil || len(*results) == 0 {
		return nil
	}
	result := (*results)[0].Result

	if tables, ok := result["tb"].(map[string]any); ok {
		for tableName := range tables {
			query := fmt.Sprintf(`REMOVE TABLE %s;`, tableName)
			_, err := surrealdb.Query[any](context.Background(), m.db, query, map[string]any{})
			if err != nil {
				return err
			}
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

	lock_doc_id := m.config.GetLockDocumentId()
	parsed_id, err := models.ParseRecordID(lock_doc_id)
	if err != nil {
		return err
	}

	query := `BEGIN; CREATE $lock_doc_id SET pid = $pid, hostname = $hostname, created_at = $created_at; RETURN AFTER; COMMIT;`

	// using surrealdb.Query looks to prevent a race condition that can occur when using surrealdb.Create
	// if you use surrealdb.Create its possible for a second lock call shortly after first to not error as it should
	_, err = surrealdb.Query[any](
		context.Background(),
		m.db,
		query,
		map[string]any{
			"lock_doc_id": parsed_id,
			"pid":         pid,
			"hostname":    hostname,
			"created_at":  time.Now().Format(time.RFC3339),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *SurrealDB) Unlock() error {
	lock_doc_id := m.config.GetLockDocumentId()
	parsed_id, err := models.ParseRecordID(lock_doc_id)
	if err != nil {
		return err
	}

	query := `BEGIN; LET $lock = SELECT * FROM $lock_doc_id; DELETE $lock; COMMIT;`

	// Delete will error if lock_doc_id does not exist because $lock ends up as NONE
	_, err = surrealdb.Query[any](context.Background(), m.db, query, map[string]any{"lock_doc_id": parsed_id})
	return err
}

func (m *SurrealDB) Run(migration io.Reader) error {
	mig, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(mig[:])
	_, err = surrealdb.Query[any](context.Background(), m.db, query, map[string]any{})
	return err
}

func (m *SurrealDB) SetVersion(version int, dirty bool) error {
	version_document_id := m.config.GetVersionDocumentId()
	parsed_id, err := models.ParseRecordID(version_document_id)
	if err != nil {
		return err
	}

	params := map[string]any{"version_document_id": parsed_id}
	query := `BEGIN; DELETE $version_document_id; `

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		params = map[string]any{
			"version_document_id": parsed_id,
			"version":             version,
			"dirty":               dirty,
		}
		query += `CREATE $version_document_id CONTENT {
					version: $version,
					dirty: $dirty
				}; `
	}

	query += `COMMIT;`

	_, err = surrealdb.Query[any](context.Background(), m.db, query, params)
	if err != nil {
		return err
	}

	return nil
}

func (m *SurrealDB) Version() (version int, dirty bool, err error) {
	version_document_id := m.config.GetVersionDocumentId()
	parsed_id, err := models.ParseRecordID(version_document_id)
	if err != nil {
		return database.NilVersion, false, err
	}

	query := "SELECT * FROM $version_document_id;"
	results, err := surrealdb.Query[[]VersionInfo](context.Background(), m.db, query, map[string]any{"version_document_id": parsed_id})
	if err != nil {
		return database.NilVersion, false, err
	}

	if results == nil || len(*results) == 0 {
		return database.NilVersion, false, nil
	}
	versionInfo := (*results)[0].Result

	if len(versionInfo) == 0 {
		return database.NilVersion, false, nil
	}

	return versionInfo[0].Version, versionInfo[0].Dirty, nil
}
