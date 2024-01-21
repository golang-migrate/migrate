# YDB

`grpc[s]://user:password@host:port/database?QUERY_PARAMS`

| URL Query  | Description |
|------------|-------------|
| `user` | The user to sign in as |
| `password` | The user's password |
| `host` | The host to connect to |
| `port` | The port to bind to |
| `database` | The name of the database to connect to |
| `x-migrations-table`| Name of the migrations table. Default: `schema_migrations` |
