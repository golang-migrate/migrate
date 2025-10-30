# Snowflake

`snowflake://user:password@accountname/dbname/schema?query`

Example URL
`snowflake://xyz:abc@ih12289.us-east-2.aws/UTIL_DB/public?x-migrations-table=schema_migrations&role=SYSADMIN&warehouse=compute_wh`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `warehouse` | | Warehouse |
| `role` | | Role of the user |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |

Snowflake is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.

## Status
This driver is not officially supported as there are no tests for it.
