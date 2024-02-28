# Turso

`turso://[DATABASE].turso.io?authToken=[TOKEN]?query`

Unlike other migrate database drivers, the turso driver will automatically wrap each migration in an implicit transaction by default. Migrations must not contain explicit `BEGIN` or `COMMIT` statements. This behavior may change in a future major release. (See below for a workaround.)

Refer to [upstream documentation](https://github.com/mattn/go-sqlite3/blob/master/README.md#connection-string) for a complete list of query parameters supported by the sqlite3 database driver. The auxiliary query parameters listed below may be supplied to tailor migrate behavior. All auxiliary query parameters are optional.

| URL Query            | WithInstance Config | Description                                                                                                              |
| -------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `x-migrations-table` | `MigrationsTable`   | Name of the migrations table. Defaults to `schema_migrations`.                                                           |
| `x-no-tx-wrap`       | `NoTxWrap`          | Disable implicit transactions when `true`. Migrations may, and should, contain explicit `BEGIN` and `COMMIT` statements. |

## Notes

- Uses the `https://github.com/libsql/go-libsql` libsql db driver (cgo)
