# YDB

`grpc://host:port/dbname?query` (plain gRPC) or `grpcs://host:port/dbname?query` (TLS gRPC)

YDB uses its own URL schemes instead of a driver name string. The default insecure port is `2136`; the default TLS port is `2135`.

## URL Query Parameters

| URL Query | WithInstance Config | Description |
|-----------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table (default: `schema_migrations`) |
| `x-statement-timeout` | `StatementTimeout` | Abort any statement that takes more than the specified number of milliseconds |
| `x-multi-statement` | `MultiStatementEnabled` | Enable multi-statement execution — multiple `;`-delimited statements per migration file (default: `false`) |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | Maximum size of a single statement in bytes when multi-statement mode is on (default: 10 MB) |
| `x-tls-certificate` | | URL-encoded PEM string of the CA certificate used to verify the server for `grpcs://` connections |
| `x-tls-certificate-file` | | Path to a PEM-encoded CA certificate file used to verify the server for `grpcs://` connections |
| `dbname` | `DatabaseName` | The YDB database path, e.g. `/local` or `/my/database` |
| `host` | | The host to connect to (default: `localhost`) |
| `port` | | The gRPC port to connect to (default: `2136`) |

### TLS (`grpcs://`)

When using `grpcs://` you must supply the CA certificate that signed the server's TLS certificate via one of:

- **`x-tls-certificate-file`** — path to a PEM file on disk (recommended):
  ```
  grpcs://my-ydb-host:2135/my/database?x-tls-certificate-file=/path/to/ca.pem
  ```

- **`x-tls-certificate`** — URL-encoded PEM string (useful for CI/CD env vars):
  ```bash
  CA_PEM=$(python3 -c "import sys,urllib.parse; print(urllib.parse.quote(open('/path/to/ca.pem').read()))")
  grpcs://my-ydb-host:2135/my/database?x-tls-certificate=${CA_PEM}
  ```

## YDB-specific Notes

- **DDL auto-detection** — `CREATE`, `ALTER`, and `DROP` statements are automatically executed in `SchemeQueryMode`; all other statements use data query mode. You do not need to annotate migrations.
- **No advisory locks** — YDB does not support advisory locks. An in-process mutex is used to prevent concurrent migrations within the same process.
- **Idempotent writes** — the driver uses `UPSERT INTO` and `DELETE FROM` for the migration tracking table instead of `INSERT`/`TRUNCATE`.
- **Type system** — YDB has its own types: `Int64`, `Uint64`, `Utf8`, `Bool`, `Date`, `Timestamp`, etc. Standard SQL types are not supported.
- **Primary key required** — every `CREATE TABLE` must include a `PRIMARY KEY` clause.
- **No `IF [NOT] EXISTS`** — `CREATE TABLE IF NOT EXISTS` and `DROP TABLE IF EXISTS` are not supported. Migrations must be written to only create/drop tables that do or do not exist at the point of execution.
- **Comment-only migrations** — migration files that contain only SQL comments are silently skipped; YDB rejects bare comments as invalid queries.

## Docker quickstart (insecure)

```bash
docker run -d --name ydb-local \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e GRPC_PORT=2136 \
  -e MON_PORT=8765 \
  -p 2136:2136 \
  -p 8765:8765 \
  cr.yandex/yc/yandex-docker-local-ydb:latest
```

```bash
migrate -database 'grpc://localhost:2136/local' -path db/migrations up
```

## Docker quickstart (TLS)

The official image supports TLS on port 2135. Mount a directory containing `ca.pem`, `cert.pem`, and `key.pem` at `/ydb_certs`:

```bash
# Generate a self-signed CA and server certificate (requires openssl)
mkdir ydb_certs
openssl genrsa -out ydb_certs/ca-key.pem 2048
openssl req -new -x509 -days 3650 -key ydb_certs/ca-key.pem \
  -subj "/CN=localhost" -out ydb_certs/ca.pem
openssl genrsa -out ydb_certs/key.pem 2048
openssl req -new -key ydb_certs/key.pem -subj "/CN=localhost" \
  | openssl x509 -req -days 3650 -CA ydb_certs/ca.pem -CAkey ydb_certs/ca-key.pem \
    -CAcreateserial \
    -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1') \
    -out ydb_certs/cert.pem

docker run -d --name ydb-local \
  --hostname localhost \
  -e GRPC_TLS_PORT=2135 \
  -e GRPC_PORT=2136 \
  -e MON_PORT=8765 \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -v "$(pwd)/ydb_certs:/ydb_certs" \
  cr.yandex/yc/yandex-docker-local-ydb:latest
```

> **Note:** The server certificate must include `IP:127.0.0.1` as a Subject Alternative Name if
> you connect via `127.0.0.1`. The `--hostname localhost` flag makes YDB advertise `localhost` as
> its node address so that `DNS:localhost` in the SAN is sufficient.

```bash
migrate -database 'grpcs://localhost:2135/local?x-tls-certificate-file=ydb_certs/ca.pem' \
  -path db/migrations up
```

## Use in your Go project

### Insecure connection

```go
import (
    "github.com/golang-migrate/migrate/v4"
    _ "github.com/golang-migrate/migrate/v4/database/ydb"
    _ "github.com/golang-migrate/migrate/v4/source/file"
)

m, err := migrate.New(
    "file://db/migrations",
    "grpc://localhost:2136/local")
```

### TLS connection

```go
m, err := migrate.New(
    "file://db/migrations",
    "grpcs://my-ydb-host:2135/my/database?x-tls-certificate-file=/path/to/ca.pem")
```

### Using an existing `*sql.DB`

```go
import (
    "database/sql"
    "github.com/golang-migrate/migrate/v4"
    "github.com/golang-migrate/migrate/v4/database/ydb"
    _ "github.com/golang-migrate/migrate/v4/source/file"
    ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

nativeDriver, _ := ydbsdk.Open(ctx, "grpcs://my-ydb-host:2135/my/database",
    ydbsdk.WithCertificateCredentials(...))
connector, _ := ydbsdk.Connector(nativeDriver,
    ydbsdk.WithAutoDeclare(),
    ydbsdk.WithPositionalArgs())
db := sql.OpenDB(connector)

driver, err := ydb.WithInstance(db, &ydb.Config{DatabaseName: "/my/database"})
m, err := migrate.NewWithDatabaseInstance("file://db/migrations", "ydb", driver)
```
