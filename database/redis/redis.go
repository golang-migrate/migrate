package redis

import (
	"context"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/redis/go-redis/v9"
	"go.uber.org/atomic"
	"io"
	neturl "net/url"
	"strconv"
	"strings"
	"time"
)

func init() {
	db := Redis{}
	database.Register("redis", &db)
	database.Register("rediss", &db)
}

var (
	DefaultMigrationsKey = "schema_migrations"
	DefaultLockKey       = "lock:schema_migrations"
	DefaultLockTimeout   = 15 * time.Second
)

func convertVersionFromDB(result []interface{}) (int, bool, error) {
	if result[0] == nil || result[1] == nil {
		return database.NilVersion, false, nil
	}

	version, err := strconv.Atoi(result[0].(string))
	if err != nil {
		return 0, false, fmt.Errorf("can't parse version: %w", err)
	}

	dirty, err := strconv.ParseBool(result[1].(string))
	if err != nil {
		return 0, false, fmt.Errorf("can't parse dirty: %w", err)
	}

	return version, dirty, nil
}

type Mode int8

const (
	ModeUnspecified Mode = iota
	ModeStandalone
	ModeFailover
	ModeCluster
)

var rawModeToMode = map[string]Mode{
	"":           ModeUnspecified,
	"standalone": ModeStandalone,
	"failover":   ModeFailover,
	"cluster":    ModeCluster,
}

func parseMode(rawMode string) (Mode, error) {
	mode, ok := rawModeToMode[strings.ToLower(rawMode)]
	if ok {
		return mode, nil
	}

	return ModeUnspecified, fmt.Errorf("unexpected mode: %q", rawMode)
}

type Config struct {
	MigrationsKey string
	LockKey       string
	LockTimeout   time.Duration
}

func newClient(url string, mode Mode) (redis.UniversalClient, error) {
	if mode == ModeUnspecified {
		var err error

		mode, err = determineMode(url)
		if err != nil {
			return nil, err
		}
	}

	switch mode {
	case ModeStandalone:
		options, err := redis.ParseURL(url)
		if err != nil {
			return nil, err
		}

		return redis.NewClient(options), nil
	case ModeFailover:
		options, err := parseFailoverURL(url)
		if err != nil {
			return nil, err
		}

		return redis.NewFailoverClient(options), nil
	case ModeCluster:
		options, err := redis.ParseClusterURL(url)
		if err != nil {
			return nil, err
		}

		return redis.NewClusterClient(options), nil
	default:
		return nil, fmt.Errorf("unexpected mode: %q", mode)
	}
}

func WithInstance(client redis.UniversalClient, config *Config) (database.Driver, error) {
	if config.MigrationsKey == "" {
		config.MigrationsKey = DefaultMigrationsKey
	}

	if config.LockKey == "" {
		config.LockKey = DefaultLockKey
	}

	if config.LockTimeout == 0 {
		config.LockTimeout = DefaultLockTimeout
	}

	return &Redis{
		client: client,
		config: config,
	}, nil
}

type Redis struct {
	client   redis.UniversalClient
	isLocked atomic.Bool
	config   *Config
}

func (r *Redis) Open(url string) (database.Driver, error) {
	purl, err := neturl.Parse(url)
	if err != nil {
		return nil, err
	}

	query := purl.Query()

	mode, err := parseMode(query.Get("x-mode"))
	if err != nil {
		return nil, err
	}

	var lockTimeout time.Duration
	rawLockTimeout := query.Get("x-lock-timeout")
	if rawLockTimeout != "" {
		lockTimeout, err = time.ParseDuration(rawLockTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid x-lock-timeout: %w", err)
		}
	}

	client, err := newClient(migrate.FilterCustomQuery(purl).String(), mode)
	if err != nil {
		return nil, fmt.Errorf("can't create client: %w", err)
	}

	return WithInstance(
		client,
		&Config{
			MigrationsKey: query.Get("x-migrations-key"),
			LockKey:       query.Get("x-lock-key"),
			LockTimeout:   lockTimeout,
		},
	)
}

func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) Lock() error {
	return database.CasRestoreOnErr(&r.isLocked, false, true, database.ErrLocked, func() error {
		return r.client.SetArgs(context.Background(), r.config.LockKey, 1, redis.SetArgs{
			Mode: "NX",
			TTL:  r.config.LockTimeout,
		}).Err()
	})
}

func (r *Redis) Unlock() error {
	return database.CasRestoreOnErr(&r.isLocked, true, false, database.ErrNotLocked, func() error {
		return r.client.Del(context.Background(), r.config.LockKey).Err()
	})
}

func (r *Redis) Run(migration io.Reader) error {
	script, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	if err = r.client.Eval(context.Background(), string(script), nil).Err(); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	return nil
}

func (r *Redis) SetVersion(version int, dirty bool) error {
	if version > 0 || (version == database.NilVersion && dirty) {
		return r.client.HMSet(context.Background(), r.config.MigrationsKey, "version", version, "dirty", dirty).Err()
	}

	return r.client.Del(context.Background(), r.config.MigrationsKey).Err()
}

func (r *Redis) Version() (version int, dirty bool, err error) {
	result, err := r.client.HMGet(context.Background(), r.config.MigrationsKey, "version", "dirty").Result()
	if err != nil {
		return 0, false, err
	}

	return convertVersionFromDB(result)
}

func (r *Redis) Drop() error {
	return r.client.FlushDB(context.Background()).Err()
}
