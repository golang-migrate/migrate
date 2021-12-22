# YDB

`ydb://host:port?token=token&database=/local&x-multi-statement=true`

| URL Query  | Description |
|------------|-------------|
| `x-migrations-table`| Name of the migrations table |
| `database` | The name of the database to connect to |
| `token` | Authentication token |
| `host` | The host to connect to |
| `port` | The port to bind to |
| `x-multi-statement` | Enable multiple statements to be ran in a single migration (See note below) |
| `x-use-grpcs-scheme` | Enable GRPCS protocol for connecting to YDB |

## Notes

* The YDB driver does not natively support executing multipe statements in a single query. To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. There are two important caveats:
  * This mode splits the migration text into separately-executed statements by a semi-colon `;`. Thus `x-multi-statement` cannot be used when a statement in the migration contains a string with a semi-colon.
  * The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
