# ClickHouse

`clickhouse://host:port?username=user&password=password&database=clicks&x-multi-statement=true`

| URL Query  | Description |
|------------|-------------|
| `x-migrations-table`| Name of the migrations table |
| `x-migrations-table-engine`| Engine to use for the migrations table, defaults to TinyLog |
| `x-cluster-name` | Name of cluster for creating `schema_migrations` table cluster wide |
| `database` | The name of the database to connect to |
| `username` | The user to sign in as |
| `password` | The user's password |
| `host` | The host to connect to. |
| `port` | The port to bind to. |
| `x-multi-statement` | false | Enable multiple statements to be ran in a single migration (See note below) |
| `x-jwt-token` | A JWT to authenticate with instead of `username` and `password` (ClickHouse Cloud, see below) |
| `x-jwt-token-file` | A file containing a JWT, read for every new connection so that a renewed token is picked up |

The driver uses [clickhouse-go v2](https://github.com/ClickHouse/clickhouse-go), so the [clickhouse-go v2 connection parameters](https://github.com/ClickHouse/clickhouse-go#dsn) such as `secure`, `skip_verify`, `dial_timeout` and `read_timeout` are accepted. Any other parameter without an `x-` prefix is sent to the server as a ClickHouse setting.

## JWT authentication

ClickHouse Cloud services can accept a JWT issued by your identity provider instead of a password. Pass the token with `x-jwt-token`, or point `x-jwt-token-file` at a file your platform keeps current, and leave out `username` and `password`:

`clickhouse://host:9440?secure=true&database=clicks&x-jwt-token-file=/var/run/secrets/clickhouse/token`

JWT authentication requires `secure=true`, so the token is never sent in clear text.

## Upgrading from clickhouse-go v1

Earlier versions of this driver used clickhouse-go v1. Existing URLs keep working:

* `read_timeout` and `timeout` given in seconds, as v1 expected, are converted (`timeout` becomes `dial_timeout`). A value of `0`, which disabled the timeout in v1, becomes a timeout of 100 years, because clickhouse-go v2 cannot disable it.
* `alt_hosts` is added to the host list, and `connection_open_strategy=time_random` becomes `random`.
* `block_size`, `no_delay`, `pool_size` and `write_timeout` have no clickhouse-go v2 equivalent and are ignored.
* `tls_config` is no longer supported: it referred to a TLS configuration registered in code. Use `secure`, `skip_verify` and `tls_server_name` instead.

If your program also uses clickhouse-go v1 directly, move it to v2 as well: both versions register the `clickhouse` database/sql driver, and a binary that links both panics at startup.

## Notes

* The Clickhouse driver does not natively support executing multiple statements in a single query. To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. There are two important caveats:
  * This mode splits the migration text into separately-executed statements by a semi-colon `;`. Thus `x-multi-statement` cannot be used when a statement in the migration contains a string with a semi-colon.
  * The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
* Using the default TinyLog table engine for the schema_versions table prevents backing up the table if using the [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup) tool. If backing up the database with make sure the migrations are run with `x-migrations-table-engine=MergeTree`.
* Clickhouse cluster mode is not officially supported, since it's not tested right now, but you can try enabling `schema_migrations` table replication by specifying a `x-cluster-name`:
  * When `x-cluster-name` is specified, `x-migrations-table-engine` also should be specified. See the docs regarding [replicated table engines](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replication/#table_engines-replication).
  * When `x-cluster-name` is specified, only the `schema_migrations` table is replicated across the cluster. You still need to write your migrations so that the application tables are replicated within the cluster.
* If you want to create database inside the migration, you should know, that table which will manage migrations `schema-migrations table` will be in `default` table, so you can't use `USE <database_name>` inside migration. In this case you may not specify the database in the connection string (example you can find [here](examples/migrations/003_create_database.up.sql))
