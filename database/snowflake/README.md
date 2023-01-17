# Snowflake

`snowflake://user:password@accountname/schema/dbname?query`

## Example

Example URL is as follows:
`snowflake://user:password@accountname/schema/dbname?role=SYSADMIN&warehouse=compute_wh`

| URL Query  | WithInstance Config | Description | Optional | Default |
|------------|---------------------|-------------|----------|---------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table | yes | schema_migrations |
| `role` | - | Name of the role | yes | `default_role` of the login user |
| `warehouse` | - | Name of the warehouse | yes | `default_warehouse` of the login user |

Snowflake is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.

## Status
This driver is not officially supported as there are no tests for it.
