# libsql

`libsql://file:path/to/database?query`
`libsql://your-turso-db.turso.io?authToken=your-auth-token`

Unlike other migrate database drivers, the libsql driver will automatically wrap each migration in an implicit transaction by default. Migrations must not contain explicit `BEGIN` or `COMMIT` statements. This behavior may change in a future major release. (See below for a workaround.)

Refer to [upstream documentation](https://github.com/tursodatabase/go-libsql#usage) for a complete list of query parameters supported by the libsql database driver. The auxiliary query parameters listed below may be supplied to tailor migrate behavior. All auxiliary query parameters are optional.

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table.  Defaults to `schema_migrations`. |
| `x-no-tx-wrap` | `NoTxWrap` | Disable implicit transactions when `true`.  Migrations may, and should, contain explicit `BEGIN` and `COMMIT` statements. |

## Notes

* Uses the `github.com/tursodatabase/go-libsql` libsql db driver.
* Supports local file databases, in-memory databases (`libsql://file::memory:`) and remote Turso databases.
```
