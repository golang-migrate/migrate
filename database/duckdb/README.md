# DuckDB

`duckdb://path/to/database.db`

| URL Query  | Description |
|------------|-------------|
| `x-migrations-table` | Name of the migrations table (default: `schema_migrations`) |

## Notes

* DuckDB is an in-process SQL OLAP database management system.
* Uses the official DuckDB Go driver: [github.com/duckdb/duckdb-go/v2](https://github.com/duckdb/duckdb-go)
* Supports in-memory databases using `:memory:` as the path.
