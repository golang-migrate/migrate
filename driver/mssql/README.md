# MSSQL Driver

* Runs migrations in transactions.
  That means that if a migration fails, it will be safely rolled back.
* Tries to return helpful error messages.
* Stores migration version details in table ``schema_migrations``.
  This table will be auto-generated.


## Usage

```bash
migrate -url="sqlserver://sa:Passw0rd@localhost:1433?database=master" -path ./db/migrations create add_field_to_table
migrate -url="sqlserver://sa:Passw0rd@localhost:1433?database=master" -path ./db/migrations up
migrate help # for more info

# see more docs: https://github.com/denisenkom/go-mssqldb
```

## Authors

* Andras Laczi, https://github.com/alaczi
