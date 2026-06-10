# SAP HANA

`hdb://user:password@host:port?TLSServerName=host&x-migrations-schema=MYSCHEMA`

## URL Parameters

| URL Query                     | WithInstance Config       | Description                                                                                                                                                     |
| ----------------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `x-migrations-schema`         | `SchemaName`              | **Required.** The schema in which the migrations table is created and migrations are applied.                                                                   |
| `x-migrations-table`          | `MigrationsTable`         | Name of the migrations table (default: `schema_migrations`).                                                                                                    |
| `x-statement-timeout`         | `StatementTimeout`        | Abort any statement that takes longer than this duration. Go duration format (e.g. `30s`, `1m`, `500ms`). Applied per-statement, not per-migration.             |
| `x-isolation-level`           | `IsolationLevel`          | Transaction isolation level as integer corresponding to [`sql.IsolationLevel`](https://pkg.go.dev/database/sql#IsolationLevel) (default: `0` = driver default). |
| `x-lock-name`                 | `LockName`                | Name of the distributed application lock used to prevent concurrent migrations (default: `migrate`).                                                            |
| `x-lock-timeout`              | `LockTimeout`             | How long to wait for the lock. Go duration format (e.g. `5s`, `30s`). `0` = no wait, error immediately if locked (default: `0`).                                |
| `x-multi-statement-delimiter` | `MultiStatementDelimiter` | Delimiter for splitting migration files into multiple statements (default: `;`).                                                                                |

## Multi-Statement Migrations

Migration files are automatically split by the configured delimiter (default `;`) and each statement is executed separately within a single transaction. This means:

- All statements in a migration file are wrapped in a single transaction for atomicity.
- If any statement fails, all previous DML statements in the same migration are rolled back.
- DDL statements (e.g. `CREATE TABLE`) auto-commit in HANA regardless of transaction boundaries.
- Empty statements (e.g. trailing delimiters, multiple consecutive delimiters) are silently skipped.

### Example with default delimiter (`;`)

```sql
CREATE ROW TABLE users (id INTEGER PRIMARY KEY);
CREATE ROW TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);
CREATE INDEX idx_orders_user ON orders (user_id);
```

### Example with custom delimiter (`--SPLIT--`)

Set `x-multi-statement-delimiter=--SPLIT--` in the URL:

```sql
CREATE ROW TABLE users (id INTEGER PRIMARY KEY)
--SPLIT--
CREATE ROW TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)
--SPLIT--
CREATE INDEX idx_orders_user ON orders (user_id)
```

## Locking

This driver uses SAP HANA's `APPLICATION_LOCK` for distributed locking. This prevents multiple processes from running migrations simultaneously.

## Usage with `WithInstance`

```go
import "github.com/golang-migrate/migrate/v4/database/hana"

db := sql.OpenDB(connector) // your pre-configured *sql.DB

driver, err := hana.WithInstance(db, &hana.Config{
    SchemaName:              "MY_SCHEMA",       // required
    MigrationsTable:         "schema_migrations",
    StatementTimeout:        30 * time.Second,
    IsolationLevel:          sql.LevelReadCommitted,
    LockName:                "my-app-migrate",
    LockTimeout:             5 * time.Second,
    MultiStatementDelimiter: ";",
})
```

## Considerations for future improvements

### Configurable Transaction Timeouts

Transactions are used where applicable to wrap multiple statements.
Currently such transactions do not use timeouts.
If required we will consider supporting configurable transaction timeouts.
