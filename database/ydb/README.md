# [YDB](https://ydb.tech/docs/en/)

`ydb://[user:password@]host:port/database?QUERY_PARAMS`

| URL Query  | Description |
|------------|-------------|
| `user` | The user to sign in as |
| `password` | The user's password |
| `host` | The host to connect to |
| `port` | The port to bind to |
| `database` | The name of the database to connect to |
| `x-migrations-table`| Name of the migrations table. Default: `schema_migrations` |
| `x-use-grpcs` | Enable GRPCS protocol for connecting to YDB (default GRPC) |

## Warning
- Beware of race conditions between migrations initiated from different processes (on the same machine or on different machines).
- Beware of partial migrations, because currently in YDB it is not possible to execute DDL SQL statements in a transaction.
