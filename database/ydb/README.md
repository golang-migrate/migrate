# [YDB](https://ydb.tech/docs/)

`ydb://[user:password@]host:port/database?QUERY_PARAMS`

| URL Query  |               Description               |
|:----------:|:---------------------------------------:|
|   `user`   |         The user to sign in as.         |
| `password` |          The user's password.           |
|   `host`   |         The host to connect to.         |
|   `port`   |          The port to connect to.           |
| `database` | The name of the database to connect to. |

|       URL Query Params       |                                         Description                                          |
|:----------------------------:|:--------------------------------------------------------------------------------------------:|
|        `x-auth-token`        |                                    Authentication token.                                     |
|     `x-migrations-table`     |                 Name of the migrations table (default `schema_migrations`).                  |
|        `x-lock-table`        |        Name of the table which maintains the migration lock (default `schema_lock`).         |
| `x-use-grpcs` | Enables TLS when bare or `true`; `false` keeps plaintext gRPC. |
|          `x-tls-ca`          |                     The location of the CA (certificate authority) file.                     |
| `x-tls-insecure-skip-verify` | Disables certificate verification when bare or `true`; default and `false` verify certificates. |
|     `x-tls-min-version`      | Controls the minimum TLS version that is acceptable, use 1.2 or 1.3 (default 1.2). |

`x-statement-timeout` sets the operation timeout in positive milliseconds (default
`300000`, five minutes). The same limit is available as `Config.StatementTimeout`
for native SDK instances. It bounds migration execution and internal SDK retries;
increase it for long migrations. Zero in `Config` selects the default.

### Secure connection

Query param `x-use-grpcs` enables secure TLS connection that requires certificates.
You can declare root certificate using ENV
variable: `export YDB_SSL_ROOT_CERTIFICATES_FILE=/path/to/ydb/certs/CA.pem` or
by using `x-tls-ca` query param: `?x-tls-ca=/path/to/ydb/certs/CA.pem`.

### Authentication

By default, golang-migrate connects to YDB
using [anonymous credentials](https://ydb.tech/docs/en/recipes/ydb-sdk/auth-anonymous). \
Through the url query, you can change the default behavior:

- To connect to YDB using [static credentials](https://ydb.tech/docs/en/recipes/ydb-sdk/auth-static) you need to specify
  username and password:
  `ydb://user:password@host:port/database`
- To connect to YDB using [token](https://ydb.tech/docs/en/recipes/ydb-sdk/auth-access-token) you need to specify token
  as query parameter:
  `ydb://host:port/database?x-auth-token=<YDB_TOKEN>`

### Locks

If golang-migrate fails to acquire the lock when no migrations are currently running, this may indicate that one of the migrations did not complete successfully.
In this case, you need to analyze the previous migrations, rollback if necessary, and manually remove the lock from the
`x-lock-table`.

### Native SDK instance

The driver uses the native YDB Query and Scheme clients. It does not require a
`database/sql` connection.

```go
ctx := context.Background()
client, err := ydb.Open(ctx, "grpc://localhost:2136/local")
if err != nil {
    return err
}

driver, err := migratedb.WithInstance(client, &migratedb.Config{})
if err != nil {
    _ = client.Close(ctx)
    return err
}
// After successful construction, driver.Close closes client.
defer driver.Close()
```

Here `ydb` is `github.com/ydb-platform/ydb-go-sdk/v3` and `migratedb` is
`github.com/golang-migrate/migrate/v4/database/ydb`.

Applications can configure other credentials on the native client before passing
it to `WithInstance`. This does not add authentication packages to migrate; the
CLI supports the anonymous, token and static credentials described above.

The migration text is executed through the Query Service. Keep each migration
compatible with the target YDB server and do not assume that a migration containing
multiple schema statements is atomic. Failed migrations retain the dirty version
until they are repaired. A failed migration execution is returned with its SQL and
YDB error details and is never replayed automatically. Acquiring a session may be
retried before execution starts. Serializable conflicts in the migration lock
transaction may be retried within the operation timeout.

### Drop

`drop` removes objects from the configured YDB database, including tables, topics
and nested directories. It preserves the database itself and its reserved `.sys` and `.metadata` directories. The driver enumerates
objects through the Scheme Service before deleting them and rejects unsupported
object types instead of reporting a complete cleanup.

### Tests

The default tests start disposable Docker containers sequentially, with at most one
test YDB container running at a time within a package test run. The external-object test
requires server support for external data sources and reports a skip if the server
explicitly disables that feature:

```sh
go test -v ./database/ydb
```

To use an existing local YDB instead:

```sh
go test -v ./database/ydb -ydb-test-dsn=grpc://localhost:2136/local
```

The existing-server tests delete all user objects in that database. Use a disposable
database. The test flag does not change the production driver's configuration.
