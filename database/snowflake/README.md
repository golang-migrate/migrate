# Snowflake

`snowflake://user:password@accountname/schema/dbname/warehouse?query`

Example URL
`snowflake://abhinav:abc@/ih12289.us-east-2.aws/public/UTIL_DB/TEST?x-migrations-table=schema_migrations&role=SYSADMIN`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `role` | | Role of the user |

Snowflake is PostgreSQL compatible but has some specific features (or lack thereof) that require slightly different behavior.
Snowflake doesn't run locally hence there are no tests. The library works against hosted instances of snowflake.
