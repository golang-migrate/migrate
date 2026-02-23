# ydb

`grpc://host:port/dbname?query` or `grpcs://host:port/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table (default: `schema_migrations`) |
| `x-statement-timeout` | `StatementTimeout` | Abort any statement that takes more than the specified number of milliseconds |
| `x-multi-statement` | `MultiStatementEnabled` | Enable multi-statement execution (default: false) |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | Maximum size of single statement in bytes (default: 10MB) |
| `dbname` | `DatabaseName` | The database path, e.g. `/local` or `/my/database` |
| `host` | | The host to connect to (default is localhost) |
| `port` | | The gRPC port to connect to (default is 2136) |

## Notes

- YDB uses `grpc://` (insecure) or `grpcs://` (TLS) URL schemes instead of a custom driver name.
- DDL statements (`CREATE`, `ALTER`, `DROP`) are automatically executed with `SchemeQueryMode`.
- YDB does not support advisory locks. An in-process atomic lock is used to prevent concurrent migrations within the same process.
- YDB uses `UPSERT INTO` for idempotent inserts and `DELETE FROM` instead of `TRUNCATE`.
- YDB column types differ from standard SQL: use `Int64`, `Uint64`, `Utf8`, `Bool`, `Date`, etc.
- Every `CREATE TABLE` must include a `PRIMARY KEY` clause.
