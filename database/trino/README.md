# Trino

The Trino driver supports schema migrations for synchronizing databases connected via Trino, including data sources like Iceberg, Parquet, and S3. It is designed to handle schema changes, but its capabilities depend on the Trino configuration.

## Connection String

The connection string for Trino follows the format:

`trino://{user}@{host}:{port}?catalog={catalog}&schema={schema}&ssl=true`

### URL Query Parameters

| Parameter | Description |
|---|---|
| `catalog` | The name of the catalog to connect to. This catalog must already exist. |
| `schema` | The name of the schema to use. This schema must already exist within the specified catalog. |
| `ssl` | A boolean value (`true` or `false`) to enable or disable SSL. If not specified, it defaults to `true` (HTTPS). |
| `x-migrations-table` | The name of the migrations table. Defaults to `schema_migrations`. |
| `x-migrations-catalog`| The catalog where the migrations table is located. If not specified, the current catalog is used. |
| `x-migrations-schema` | The schema where the migrations table is located. If not specified, the current schema is used. |
| `x-statement-timeout` | The statement timeout in milliseconds. |

### Notes

- **Pre-existing Catalog and Schema**: The catalog and schema specified in the connection string must be created in Trino beforehand. The driver does not create them automatically.
- **Schema Synchronization**: The primary purpose of this driver is to synchronize schemas across different databases connected through Trino. It is particularly useful for managing schema evolution in data lakes where data is stored in formats like Iceberg, Parquet, or on S3.
- **Schema Changes**: Support for schema changes (e.g., `ALTER TABLE`) is dependent on the underlying connector and data source configuration in Trino.