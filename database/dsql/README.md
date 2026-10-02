# Amazon Aurora DSQL

This driver runs migrations on [Amazon Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/what-is-aurora-dsql.html)
using IAM authentication and waits for asynchronous DDL before recording a migration as applied.

## Usage

```sh
go build -tags dsql ./cmd/migrate
migrate -source file://migrations \
  -database 'dsql://admin@mycluster.dsql.us-east-1.on.aws/postgres' up
```

The [Aurora DSQL connector for pgx](https://github.com/awslabs/aurora-dsql-connectors/tree/main/go/pgx)
generates IAM tokens and enforces TLS, so the URL has no password. Use `dsql:DbConnectAdmin`
for `admin` or `dsql:DbConnect` for a [custom role](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/using-database-and-iam-roles.html).

URL: `dsql://user@cluster.dsql.region.on.aws:5432/postgres?query`

Connector options:

| URL query | Default | Description |
|---|---|---|
| `region` | parsed from host | AWS region |
| `profile` | default chain | AWS shared-config profile |
| `tokenDurationSecs` | `900` | IAM token validity |

Driver options:

| URL query | `WithInstance` config | Default | Description |
|---|---|---|---|
| `x-migrations-schema` | `SchemaName` | `CURRENT_SCHEMA()` | Schema for driver tables |
| `x-migrations-table` | `MigrationsTable` | `schema_migrations` | Migrations table |
| `x-lock-table` | `LockTable` | `schema_migrations_lock` | Lock table |
| `x-force-lock` | `ForceLock` | `false` | Release an existing lock row |
| `x-statement-timeout` | `StatementTimeout` | none | Statement timeout in milliseconds |
| `x-multi-statement` | `MultiStatementEnabled` | `false` | Split a file on literal `;` characters |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | 10 MB | Maximum buffered statement size |
| `x-occ-max-retries` | `OCCMaxRetries` | `0` | OCC conflict retries |
| `x-occ-max-retry-delay` | `OCCMaxRetryDelay` | `5000` | Maximum retry delay in milliseconds |

Use `search_path` for unqualified SQL. Other connection settings require `WithInstance`.

## Example

Example [migration file](../../MIGRATIONS.md):

```sql
-- 1_create_users.up.sql
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL
);

-- 1_create_users.down.sql
DROP TABLE IF EXISTS users;
```

See the complete [example migrations](examples/migrations), including
[`CREATE INDEX ASYNC`](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html).

## Notes

- Prefer one DDL statement per file. Multi-statement mode uses a literal `;` splitter and
  runs each statement in a separate transaction.
- Validate migration SQL with [dsql-lint](https://github.com/awslabs/aurora-dsql-tools/tree/main/dsql-lint).
- `x-occ-max-retries` retries [optimistic concurrency](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html)
  conflicts.
- `x-force-lock=true` releases a stale migration lock; first confirm that no migration is running.
- Review the [migration guide](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html),
  [unsupported PostgreSQL features](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-unsupported-features.html),
  and [database limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)
  before writing migrations.

## Development

```sh
go test ./database/dsql/
go test -run TestPostgresStandIn ./database/dsql/
DSQL_CLUSTER_ENDPOINT=mycluster.dsql.us-east-1.on.aws \
DSQL_TEST_SCHEMA=migrate_scratch \
  go test -run TestIntegration ./database/dsql/
```

The PostgreSQL and DSQL tests skip when their dependencies are unavailable. `DSQL_TEST_SCHEMA`
must be a disposable existing schema because integration tests can remove every object in it.
