# yugabytedb

`yugabytedb://user:password@host:port/dbname?query` (`yugabyte://`, and `ysql://` work, too)

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `x-lock-table` | `LockTable` | Name of the table which maintains the migration lock |
| `x-force-lock` | `ForceLock` | Force lock acquisition to fix faulty migrations which may not have released the schema lock (Boolean, default is `false`) |
| `x-max-retries` | `MaxRetries` | How many times retry queries on retryable errors (40001, 40P01, 08006, XX000). Default is 10 |
| `x-max-retry-interval` | `MaxRetryInterval` | Interval between retries increases exponentially. This option specifies maximum duration between retries. Default is 15s |
| `x-max-retry-elapsed-time` | `MaxRetryElapsedTime` | Total retries timeout. Default is 30s |
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as |
| `password` | | The user's password |
| `host` | | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost) |
| `port` | | The port to bind to. (default is 5432) |
| `connect_timeout` | | Maximum wait for connection, in seconds. Zero or not specified means wait indefinitely. |
| `sslcert` | | Cert file location. The file must contain PEM encoded data. |
| `sslkey` | | Key file location. The file must contain PEM encoded data. |
| `sslrootcert` | | The location of the root certificate file. The file must contain PEM encoded data. |
| `sslmode` | | Whether or not to use SSL (disable\|require\|verify-ca\|verify-full) |
