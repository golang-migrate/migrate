# SAP HANA

`hdb://user:password@host:port?TLSServerName=host&x-migrations-schema=MYSCHEMA`

## URL Parameters

| URL Query              | WithInstance Config  | Description |
|------------------------|----------------------|-------------|
| `x-migrations-schema`  |                      | **Required.** The schema in which the migrations table is created and migrations are applied. |
| `x-migrations-table`   | `MigrationsTable`    | Name of the migrations table. (default: `schema_migrations`) |
| `x-statement-timeout`  | `StatementTimeout`   | Abort any statement that takes longer than this duration (e.g. `30s`, `1m`). |
| `x-isolation-level`    | `IsolationLevel`     | Transaction isolation level as an integer corresponding to [`sql.IsolationLevel`](https://pkg.go.dev/database/sql#IsolationLevel). (default: `0`) |
