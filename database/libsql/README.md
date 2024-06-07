# libsql

To use with Turso, you can use the URL provided by Turso like this:

`libsql://libsql://[DATABASE].turso.io?authToken=[TOKEN]&query`

The first `libsql://` is used by `migrate` to use the correct driver. The 2nd one is used by `libsql` internally to select either HTTP or HTTPS scheme (https://github.com/tursodatabase/libsql-client-go/blob/4ae0eb9d0898e03e96490c91c9a8c55d1167684d/libsql/sql.go#L109)

You can also use any of these schemes as needed:
`libsql://, https://, http://, wss:// and ws://`

Unlike other migrate database drivers, the libsql driver will automatically wrap each migration in an implicit transaction by default. Migrations must not contain explicit `BEGIN` or `COMMIT` statements. This behavior may change in a future major release. (See below for a workaround.)

Refer to [upstream documentation](https://github.com/mattn/go-sqlite3/blob/master/README.md#connection-string) for a complete list of query parameters supported by the sqlite3 database driver. The auxiliary query parameters listed below may be supplied to tailor migrate behavior. All auxiliary query parameters are optional.

| URL Query            | WithInstance Config | Description                                                                                                              |
| -------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `x-migrations-table` | `MigrationsTable`   | Name of the migrations table. Defaults to `schema_migrations`.                                                           |
| `x-no-tx-wrap`       | `NoTxWrap`          | Disable implicit transactions when `true`. Migrations may, and should, contain explicit `BEGIN` and `COMMIT` statements. |

## Local dev

- Run Turso locally

`turso dev --db-file test.db`

- Use migrate command

`migrate -source file://./database/libsql/examples/migrations/ -verbose --database "libsql://http://localhost:8080" up`

## Notes

- Uses the `github.com/tursodatabase/libsql-client-go/libsql` libsql db driver
