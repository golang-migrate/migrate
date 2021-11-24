# Snowflake

`snowflake://user:password@accountname/schema/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |

Snowflake is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.

## Status
This driver is not officially supported as there are no tests for it.
