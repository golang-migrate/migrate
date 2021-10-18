package tarantool

import (
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
	"github.com/tarantool/go-tarantool"
	"go.uber.org/atomic"
)

var DefaultMigrationsTable = "schema_migrations"

func init() {
	db := &Tarantool{}
	database.Register("tarantool", db)
}

type Config struct {
	MigrationsTable       string
	KeyspaceName          string
	NoLock                bool
	MultiStatementMaxSize int
}

type Tarantool struct {
	client   *tarantool.Connection
	isLocked atomic.Bool
	config   *Config
}

func WithInstance(_client *tarantool.Connection, _config *Config) (database.Driver, error) {
	if _config == nil {
		return nil, fmt.Errorf("config not set")
	}

	if _client.ClosedNow() {
		return nil, fmt.Errorf("session is closed by user or after reconnect")
	}

	if _config.KeyspaceName == "" {
		return nil, fmt.Errorf("keyspace name is empty")
	}

	if len(_config.MigrationsTable) == 0 {
		_config.MigrationsTable = DefaultMigrationsTable
	}

	tr := &Tarantool{
		client: _client,
		config: _config,
	}

	if err := tr.checkVersionSpace(); err != nil {
		return nil, err
	}

	return tr, nil
}

func (tr *Tarantool) Open(_url string) (database.Driver, error) {
	u, err := url.Parse(_url)
	if err != nil {
		return nil, err
	}
	if len(u.Path) == 0 {
		return nil, fmt.Errorf("no url provided")
	}

	opts := tarantool.Opts{}

	if len(u.Query().Get("user")) > 0 && len(u.Query().Get("pass")) > 0 {
		opts.User, opts.Pass = u.Query().Get("user"), u.Query().Get("pass")
	}
	if len(u.Query().Get("timeout")) > 0 {
		var _timeout time.Duration
		_timeout, err = time.ParseDuration(u.Query().Get("timeout")) // in 10^-9
		if err != nil {
			return nil, err
		}
		opts.Timeout = _timeout
	}
	if len(u.Query().Get("reconnect")) > 0 {
		var _reconnect time.Duration
		_reconnect, err = time.ParseDuration(u.Query().Get("reconnect"))
		if err != nil {
			return nil, err
		}
		opts.Reconnect = _reconnect
	}

	tr.client, err = tarantool.Connect(u.Host, opts)
	if err != nil {
		return nil, err
	}
	tr.config = &Config{
		KeyspaceName: strings.TrimPrefix(u.Path, "/"),
	}
	if query := u.Query().Get("x-migrations-table"); query != "" {
		tr.config.MigrationsTable = u.Query().Get("x-migrations-table")
	}
	return WithInstance(tr.client, tr.config)
}

func (tr *Tarantool) Close() error {
	if err := tr.client.Close(); err != nil {
		return err
	}
	return nil
}

func (tr *Tarantool) Lock() error {
	if !tr.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (tr *Tarantool) Unlock() error {
	if !tr.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (tr *Tarantool) Run(migration io.Reader) error {
	var dbErr error
	multiStmtDelimiter := []byte(";")
	if err := multistmt.Parse(migration, multiStmtDelimiter, tr.config.MultiStatementMaxSize, func(migration []byte) bool {
		query := strings.TrimSpace(string(migration))
		if query == "" {
			return true
		}
		if _, err := tr.client.Call(query, []interface{}{}); err != nil { //	i would find a better and clean way
			dbErr = database.Error{
				Query:   migration,
				Err:     "migration failed",
				OrigErr: err,
			}
			return false
		}
		return true
	}); err != nil {
		return err
	}
	return dbErr
}

func (tr *Tarantool) Drop() error {
	query := "box.space." + tr.config.KeyspaceName + ":drop"
	_, err := tr.client.Call(query, []interface{}{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "drop space failed", Query: []byte(query)}
	}
	return nil
}

func (tr *Tarantool) SetVersion(version int, dirty bool) error {
	query := "box.space." + tr.config.MigrationsTable + ":truncate"
	_, err := tr.client.Call(query, []interface{}{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: err.Error(), Query: []byte(query)}
	}
	if version >= 0 || (version == database.NilVersion && dirty) {
		_, err := tr.client.Call("box.space."+tr.config.MigrationsTable+":insert", []interface{}{
			[]interface{}{version, dirty},
		})
		if err != nil {
			return &database.Error{OrigErr: err, Err: "insert in migration space failed", Query: []byte(query)}
		}
	}
	return nil
}

func (tr *Tarantool) Version() (version int, dirty bool, err error) {

	resp, err := tr.client.Call17("box.space."+tr.config.MigrationsTable+":select", []interface{}{
		[]interface{}{},
		map[string]interface{}{"iterator": "LT"},
	})
	if err != nil || fmt.Sprintf("%v", resp.Data[0]) == "[]" {
		return database.NilVersion, false, err
	}
	var tmp interface{}
	switch t := resp.Data[0].(type) {
	case []interface{}:
		for _, value := range t {
			tmp = value
		}
	}
	version, dirty = MsgUnpack(tmp)
	return version, dirty, nil
}

func (tr *Tarantool) checkVersionSpace() (err error) {
	if err := tr.Lock(); err != nil {
		return err
	}

	defer func() {
		if unErr := tr.Unlock(); unErr != nil {
			err = multierror.Append(err, unErr)
		}
	}()

	tr.client.Call("box.schema.space.create", []interface{}{
		tr.config.MigrationsTable,
		map[string]bool{"if_not_exists": true},
	})

	if _, err := tr.client.Call("box.space."+tr.config.MigrationsTable+":format", [][]map[string]string{
		{
			{"name": "version", "type": "integer"},
			{"name": "dirty", "type": "boolean"},
		}}); err != nil {
		return err
	}

	if _, err := tr.client.Call("box.space."+tr.config.MigrationsTable+":create_index", []interface{}{
		"version_index",
		map[string]interface{}{
			"type":          "tree",
			"parts":         []string{"version", "dirty"},
			"if_not_exists": true},
	}); err != nil {
		return err
	}
	return nil
}

func MsgUnpack(t interface{}) (version int, dirty bool) {
	switch reflect.TypeOf(t).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(t)
		switch reflect.TypeOf(s.Index(0).Interface()).Kind() {
		case reflect.Int64:
			version = int(s.Index(0).Interface().(int64))
		case reflect.Uint64:
			version = int(s.Index(0).Interface().(uint64))
		}
		dirty = s.Index(1).Interface().(bool)
	}
	return
}
