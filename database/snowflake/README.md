# Snowflake

`snowflake://user:password@accountname/schema/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-warehouse`| n/a | Name of the warehouse to use when connecting |
| `x-role`     | n/a | Name of the role to use when connecting |
| `x-multi-statement` | `MultiStatementEnabled` | Enable multiple statements to be run in a single migration.  Defaults to `false` |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `x-timeout` | n/a | Request timeout. Defaults to 5 minutes |
| `x-connect-timeout` | `ConnectTimeout` | Initial connection timeout to the cluster. Defaults to 30 seconds |

Snowflake is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.

## Status
This driver is not officially supported.
