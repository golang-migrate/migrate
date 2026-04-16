# turso

`turso://path/to/database?query`

Driver for [Turso](https://github.com/tursodatabase/turso) -- Turso's CGO-free Rust rewrite of SQLite. Uses the [`turso-go`](https://pkg.go.dev/turso.tech/database/tursogo) bindings.

Unlike most migrate database drivers, the turso driver wraps each migration in an implicit transaction by default. Migrations must not contain explicit `BEGIN` or `COMMIT` statements. Use `x-no-tx-wrap=true` for migrations that manage their own transactions (e.g. those using `BEGIN CONCURRENT` for MVCC).

## URL Query Parameters

| URL Query | WithInstance Config | Description |
| --- | --- | --- |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table. Defaults to `schema_migrations`. |
| `x-no-tx-wrap` | `NoTxWrap` | Disable implicit transactions when `true`. |
| `x-busy-timeout` | `BusyTimeout` | Busy timeout in milliseconds. Default `5000`. Set `-1` to disable. Maps to `_busy_timeout` in the turso-go DSN. |
| `x-experimental` | `Experimental` | Comma-separated list of turso-go experimental features. Known values: `encryption`, `custom_types`, `index_method` (FTS), `fts`, `mvcc`. Maps to `experimental` in the turso-go DSN. |
| `x-encryption-cipher` | `EncryptionCipher` | Encryption cipher (e.g. `aegis256`). Requires `encryption` in `x-experimental`. Maps to `encryption_cipher` in the turso-go DSN. |
| `x-encryption-hexkey` | `EncryptionHexkey` | Hex-encoded encryption key (64 hex chars for AEGIS-256). Maps to `encryption_hexkey` in the turso-go DSN. |
| `x-vfs` | `Vfs` | VFS backend: `memory`, `syscall`, `io_uring` (Linux), `experimental_win_iocp` (Windows). Maps to `vfs` in the turso-go DSN. |
| `x-async` | `AsyncIO` | Enable async IO mode (`true`/`false`). Maps to `async` in the turso-go DSN. |


**Note:** The experimental flags list is valid as of [Turso v0.5.3](https://github.com/tursodatabase/turso/releases/tag/v0.5.3) in April 2026.
Turso is still in Beta, and the list of experimental features may have changed
since then. See [Turso Docs](https://docs.turso.tech/sql-reference/experimental-features) for the most up to date list of experimental feature flags.

## Notes

- **CGO-free.** Uses `turso.tech/database/tursogo`, which calls into Rust via [purego](https://github.com/ebitengine/purego). No `gcc` required, no build flags.
- **Beta software.** Turso is in beta. Use caution with production data and ensure backups.
- **Custom types** like `uuid`, `boolean`, `varchar`, `timestamp`, `json`, `jsonb`, `date`, `time`, `smallint`, `bigint`, `inet` are stable in Turso v0.5.0 but **still require `x-experimental=custom_types`** in turso-go v0.5.3. Verified: without the flag, custom types fail with `"unknown datatype"`.
- **MVCC** removes the concurrent writes limitation of Sqlite, however, as of v0.5.3, it does not work with custom types and strict tables. See [Turso Docs](https://docs.turso.tech/tursodb/concurrent-writes) for more information.
- **STRICT tables** are supported (verified with turso-go v0.5.3).
- **VACUUM** is not supported by Turso (`"VACUUM is not supported yet"`). The driver's `Drop()` method omits the `VACUUM` step that the sqlite3 driver includes. Use `VACUUM INTO 'filename'` if you need to compact a database.
- **In-memory databases:** use `turso://:memory:`.
- **`sqlite_master`** works for schema introspection (verified).
- **Multi-statement migrations** work natively -- turso-go's `ExecContext` handles multiple statements in a single call without requiring statement splitting.

## Custom types example

```sql

-- A type that stores monetary values as cents
CREATE TYPE cents BASE integer
    ENCODE value * 100
    DECODE value / 100;

CREATE TABLE users (
    id uuid PRIMARY KEY,
    email varchar(255) NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    account_balance cents NOT NULL DEFAULT 0
) STRICT;
```

```bash
migrate -database "turso:///path/to/db?x-experimental=custom_types" \
  -path ./migrations up
```

See [Data Types Documentation](https://docs.turso.tech/sql-reference/data-types)
for more information on usage, creating custom types, and built in custom types.

## Encryption

```bash
migrate -database "turso:///path/to/db?x-experimental=encryption,custom_types&x-encryption-cipher=aegis256&x-encryption-hexkey=<64-hex>" \
  -path ./migrations up
```

The hex key must be 64 characters (32 bytes) for AEGIS-256. Generate one with:
```bash
openssl rand -hex 32
```

Or programmatically:
```go
import (
    "github.com/golang-migrate/migrate/v4"
    "github.com/golang-migrate/migrate/v4/database/turso"
    "database/sql"
    _ "turso.tech/database/tursogo"
)

dsn := "/path/to/db?experimental=encryption,custom_types&encryption_cipher=aegis256&encryption_hexkey=" + hexKey
db, _ := sql.Open("turso", dsn)
driver, _ := turso.WithInstance(db, &turso.Config{})
m, _ := migrate.NewWithDatabaseInstance("file://./migrations", "turso", driver)
m.Up()
```

See [Encryption Docs](https://docs.turso.tech/tursodb/encryption) for more
information and list of supported ciphers.

## MVCC concurrent writes

```sql
-- migration with x-no-tx-wrap=true
PRAGMA journal_mode = 'mvcc';
BEGIN CONCURRENT;
-- ... statements ...
COMMIT;
```

Users wanting MVCC inside a single migration must set `x-no-tx-wrap=true`, since the implicit transaction wrap will conflict with `BEGIN CONCURRENT`.

## Full-text search

Enable via `x-experimental=index_method`, then in your migration:
```sql
CREATE INDEX idx_posts_search ON posts USING fts (title, body);
```

## DSN parameter mapping

When using `Open(url)`, the driver translates `x-*` URL query parameters to turso-go DSN parameters:

| URL param | turso-go DSN param | Notes |
| --- | --- | --- |
| `x-experimental` | `experimental` | No underscore prefix |
| `x-encryption-cipher` | `encryption_cipher` | No underscore prefix |
| `x-encryption-hexkey` | `encryption_hexkey` | No underscore prefix |
| `x-busy-timeout` | `_busy_timeout` | Has underscore prefix |
| `x-vfs` | `vfs` | No underscore prefix (verified in turso-go source) |
| `x-async` | `async` | No underscore prefix |

When using `WithInstance(db, config)`, the caller constructs the `*sql.DB` with the desired DSN directly. The Turso-specific `Config` fields are advisory only in this mode -- the encryption, VFS, etc. are already baked into the connection.

## Acknowledgements

This driver was created in response to [issue #1382](https://github.com/golang-migrate/migrate/issues/1382).
