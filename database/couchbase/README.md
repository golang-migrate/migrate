# couchbase

`couchbase://user:password@host:port/bucket?query` (`couchbases://` for TLS)

Migrations are JSON files containing arrays of N1QL statements:

```json
[
  {"query": "CREATE PRIMARY INDEX IF NOT EXISTS ON `mybucket`.`_default`.`_default`"},
  {"query": "UPSERT INTO `mybucket` (KEY, VALUE) VALUES ($1, $2)", "params": ["doc::1", {"name": "Alice"}]}
]
```

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-scope` | `ScopeName` | Couchbase scope for migration metadata (default: `_default`) |
| `x-migrations-collection` | `MigrationsCollection` | Collection to store migration version (default: `migrations`) |
| `x-advisory-locking` | `Locking.Enabled` | Enable advisory locking (default: `true`) |
| `x-advisory-lock-collection` | `Locking.CollectionName` | Collection for lock document (default: `migrate_advisory_lock`) |
| `x-advisory-lock-timeout` | `Locking.Timeout` | Seconds to wait for lock acquisition (default: `15`) |
| `x-advisory-lock-timeout-interval` | `Locking.Interval` | Max backoff interval in seconds between lock retries (default: `10`) |

## Usage

### CLI

```bash
# Apply all up migrations
migrate -source file://migrations -database "couchbase://Administrator:password@localhost:11210/mybucket?x-scope=_default" up

# Roll back one migration
migrate -source file://migrations -database "couchbase://Administrator:password@localhost:11210/mybucket" down 1

# Force a specific version (useful for fixing dirty state)
migrate -source file://migrations -database "couchbase://Administrator:password@localhost:11210/mybucket" force 3

# TLS connection
migrate -source file://migrations -database "couchbases://Administrator:password@localhost:11207/mybucket" up
```

### Go

```go
import (
    "github.com/couchbase/gocb/v2"
    "github.com/golang-migrate/migrate/v4"
    "github.com/golang-migrate/migrate/v4/database/couchbase"
    _ "github.com/golang-migrate/migrate/v4/source/file"
)

// Using a URL
m, err := migrate.New("file://migrations", "couchbase://user:pass@localhost:11210/mybucket")

// Using an existing cluster instance
cluster, _ := gocb.Connect("couchbase://localhost", gocb.ClusterOptions{
    Authenticator: gocb.PasswordAuthenticator{
        Username: "Administrator",
        Password: "password",
    },
})

driver, err := couchbase.WithInstance(cluster, &couchbase.Config{
    BucketName: "mybucket",
    ScopeName:  "_default",
    Locking: couchbase.Locking{
        Enabled: true,
        Timeout: 15,
    },
})

m, err := migrate.NewWithDatabaseInstance("file://migrations", "mybucket", driver)
m.Up()
```

## Migration file format

Each migration file is a JSON array of objects. Each object has a `query` field (required) and an optional `params` field for positional parameters:

```json
[
  {"query": "CREATE COLLECTION `mybucket`.`myscope`.`users` IF NOT EXISTS"},
  {"query": "CREATE INDEX idx_email IF NOT EXISTS ON `mybucket`.`myscope`.`users`(email)"},
  {"query": "UPSERT INTO `mybucket` (KEY, VALUE) VALUES ($1, $2)", "params": ["key1", {"field": "value"}]}
]
```

See the [examples/migrations](examples/migrations) directory for a complete set of migrations covering collections, indexes, and data operations.

## Advisory locking

Advisory locking is enabled by default and uses a document-based lock in a dedicated collection. This prevents concurrent migration runs from corrupting state. The lock uses exponential backoff when waiting for acquisition.

To disable locking (e.g., for single-instance deployments):

```
couchbase://user:pass@host/bucket?x-advisory-locking=false
```

## Couchbase version support

Tested against:
- Couchbase Community Edition 7.6.2
- Couchbase Community Edition 8.0.0
