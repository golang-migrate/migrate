# Amazon Aurora DSQL

`dsql://user:password@host:port/postgres?query`

Aurora DSQL uses PostgreSQL's wire protocol but has different locking and transaction semantics. This driver uses `pgx/v5`, stores the migration lock in a table instead of using advisory locks, replaces `TRUNCATE` with `DELETE`, and can execute semicolon-delimited migration statements separately so each DDL statement has its own transaction.

| URL query | `WithInstance` config | Description |
|---|---|---|
| `x-migrations-table` | `MigrationsTable` | Migration version table (default: `schema_migrations`) |
| `x-lock-table` | `LockTable` | Migration lock table (default: `schema_migrations_lock`) |
| `x-force-lock` | `ForceLock` | Remove an existing lock row before acquiring the lock. Use only to recover a stale lock. |
| `x-statement-timeout` | `StatementTimeout` | Abort a statement after this many milliseconds. |
| `x-multi-statement` | `MultiStatementEnabled` | Split migration files at semicolons so DDL statements execute separately (default: `false`). |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | Maximum size of one parsed statement (default: 10 MB). |
| `search_path` | `SchemaName` | Schema containing the migration and lock tables. |

## Authentication

Generate an IAM authentication token and URL-encode it before placing it in the connection URL:

```shell
endpoint="your-cluster-id.dsql.us-east-1.on.aws"
token="$(aws dsql generate-db-connect-admin-auth-token --hostname "$endpoint" --region us-east-1)"
encoded_token="$(printf '%s' "$token" | jq -sRr @uri)"

migrate \
  -source file://path/to/migrations \
  -database "dsql://admin:${encoded_token}@${endpoint}:5432/postgres?sslmode=verify-full" \
  up
```

The token is used when the connection is established. This driver does not generate or refresh IAM credentials.

## DSQL migration rules

- Keep DDL and DML in separate migration files.
- Don't wrap migrations in explicit `BEGIN`/`COMMIT` blocks.
- Use `CREATE INDEX ASYNC`, not `CREATE INDEX`.
- Keep write transactions within Aurora DSQL's row, size, and duration limits.
- Prefer one DDL statement per migration file.
- Set `x-multi-statement=true` only when a migration file contains several DDL statements that must execute separately. The parser splits at every semicolon, including semicolons in SQL function bodies and string literals.
- A terminated migration process can leave a lock row. After confirming no migration is active, run once with `x-force-lock=true` to recover it.

See the [Aurora DSQL PostgreSQL migration guide](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html) for current compatibility details.
