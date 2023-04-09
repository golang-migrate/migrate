# SurrealDB

`surrealdb://username:password@host:port/namespace/database` (`surreal://` works, too)

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table.  Defaults to `schema_migrations`. |
| `namespace` | `Namespace` | The name of the database to connect to |
| `database` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as |
| `password` | | The user's password | 
| `host` | | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost) |
| `port` | | The port to bind to. (default is 5432) |
| `sslmode` | | Whether or not to use SSL (disable\|require) |

## Notes

* Uses the `github.com/surrealdb/surrealdb.go` surrealdb driver
