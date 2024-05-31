# YDB

See [YDB Documentation](https://ydb.tech/docs/en/) for more details.

## Usage

The DSN must be given in the following format.

`ydb://{endpoint}/{database}[?param=value]`

| Param | WithInstance Config | Description |
| ----- | ------------------- | ----------- |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table. Defaults to `schema_migrations`. |
| `x-insecure` | | Boolean to indicate whether to use an insecure connection. Defaults to `false`. |
| `x-connect-timeout` | | Initial connection timeout to the cluster. Unset means no timeout. |

`x-connect-timeout` is parsed using [time.ParseDuration](https://pkg.go.dev/time#ParseDuration)
