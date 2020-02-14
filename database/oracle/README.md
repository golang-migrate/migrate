# oracle
The golang oracle driver (go-oci8) does not natively support executing multiple statements in a single query. 
To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. 
This mode splits the migration text into separately-executed statements:
1. If there is no PL/SQL statement in a migration file, the `semicolons` will be the separator
2. If there is any PL/SQL statement in a migration file, the separator will be `---` in a single line or specified by `x-plsql-line-separator`, 
   And in this case the multiple statements cannot be used when a statement in the migration contains a string with the given `line separator`.


`oracle://user/password@host:port/sid?query` (`oci8` works too)

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table in UPPER case |
| `x-plsql-line-separator` | `PLSQLStatementSeparator` | a single line which use as the token to spilt multiple statements in single migration file (See note above), default `---` |

## Building

You'll need to [Install Oracle full client or Instant Client:](https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html) for oracle support since this uses [github.com/mattn/go-oci8](https://github.com/mattn/go-oci8)
