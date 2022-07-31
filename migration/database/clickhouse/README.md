# ClickHouse

`clickhouse://username:password@host:port/database=clicks?x-multi-statement=true`

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

## Notes

* The Clickhouse driver does not natively support executing multipe statements in a single query. To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. There are two important caveats:
  * This mode splits the migration text into separately-executed statements by a semi-colon `;`. Thus `x-multi-statement` cannot be used when a statement in the migration contains a string with a semi-colon.
  * The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
* Using the default TinyLog table engine for the schema_versions table prevents backing up the table if using the [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup) tool. If backing up the database with make sure the migrations are run with `x-migrations-table-engine=MergeTree`.
* Clickhouse cluster mode is not officially supported, since it's not tested right now, but you can try enabling `schema_migrations` table replication by specifying a `x-cluster-name`:
  * When `x-cluster-name` is specified, `x-migrations-table-engine` also should be specified. See the docs regarding [replicated table engines](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replication/#table_engines-replication).
  * When `x-cluster-name` is specified, only the `schema_migrations` table is replicated across the cluster. You still need to write your migrations so that the application tables are replicated within the cluster.
