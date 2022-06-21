# immudb

`immudb://user:password@host:port/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `x-advisory-lock-key` | `LockKey` | Name of the migrations table |
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as |
| `password` | | The user's password | 
| `host` | | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost) |
| `port` | | The port to bind to. (default is 5432) |

## Notes

* Immudb supports executing multiple statements in a single file natively.
* Immudb supports transactions, but this package does not implement them.
