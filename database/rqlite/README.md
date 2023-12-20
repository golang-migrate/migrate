# rqlite

`rqlite://admin:secret@server1.example.com:4001/?level=strong&timeout=5`

The `rqlite` url scheme is used for both secure and insecure connections. If connecting to an insecure database, pass `x-connect-insecure` in your URL query, or use `WithInstance` to pass an established connection.

The migrations table name is configurable through the `x-migrations-table` URL query parameter, or by using `WithInstance` and passing `MigrationsTable` through `Config`.

Other connect parameters are directly passed through to the database driver. For examples of connection strings, see https://github.com/rqlite/gorqlite#examples.

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-connect-insecure` | n/a: set on instance | Boolean to indicate whether to use an insecure connection. Defaults to `false`. |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table.  Defaults to `schema_migrations`. |

## Notes

* Uses the https://github.com/rqlite/gorqlite driver
