# libsql

libsql implements the [libsql-client-go](https://github.com/libsql/libsql-client-go) interface for use with [migrate](https://github.com/golang-migrate/migrate).

It can be used to connect to any database supported by libsql:

- Local SQLite database files (See [Notes](#notes))
- libSQL sqld instances (including Turso)

## Notes

- Uses the `github.com/libsql/libsql-client-go` libsql db driver (go)
  - [No support for prepared statements using sqld with https](https://github.com/libsql/libsql-client-go/#compatibility-with-databasesql)