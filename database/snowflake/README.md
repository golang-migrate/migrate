# Snowflake

`snowflake://user:password@accountname/schema/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |

Snowflake is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.
Snowflake doesn't run locally hence there are no tests. The library works against hosted instances of snowflake.
