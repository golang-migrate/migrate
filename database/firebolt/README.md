# Firebolt

`firebolt:///database?client_id=id&client_secret=secret&account_name=account&engine=engine&x-multi-statement=true`

| URL Query  | Description |
|------------|-------------|
| `client_id` | Client ID of the Firebolt service account |
| `client_secret` | Client secret of the Firebolt service account |
| `account_name` | Firebolt account name |
| `database` | The name of the database (specified in the URL path as `/database`) |
| `engine` | The name of the engine to run queries on (optional) |
| `x-migrations-table` | Name of the migrations table (default: `schema_migrations`) |
| `x-multi-statement` | Enable multiple statements in a single migration (default: `false`) |
| `x-multi-statement-max-size` | Maximum size of a single migration when multi-statement is enabled (default: 10 MB) |

## Notes

* Integration tests require a running Firebolt instance. Set `FIREBOLT_DSN` to a valid Firebolt DSN:
  * Core: `FIREBOLT_DSN='firebolt://?url=http://localhost:3473'`
  * Cloud: `FIREBOLT_DSN='firebolt:///mydb?client_id=...&client_secret=...&account_name=...&engine=...'`
* The Firebolt driver does not natively support executing multiple statements in a single query. To allow for multiple statements in a single migration, you can use the `x-multi-statement` query parameter. There are two important caveats:
  * This mode splits the migration text into separately-executed statements by a semi-colon `;`. Thus `x-multi-statement` cannot be used when a statement in the migration contains a string with a semi-colon.
  * The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
* The migrations table uses Firebolt's native `BOOLEAN` type for the dirty flag and `BIGINT` for version and sequence tracking.
* The `Drop` method removes all `BASE TABLE` entries from the connected database using `information_schema.tables`.
