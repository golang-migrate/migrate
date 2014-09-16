# PostgreSQL Driver

* Runs migrations in transcations.
  That means that if a migration failes, it will be safely rolled back.
* Tries to return helpful error messages.
* Stores migration version details in table ``schema_migrations``.
  This table will be auto-generated.


## Usage

```bash
migrate -url postgres://user@host:port/database -path ./db/migrations create add_field_to_table
migrate -url postgres://user@host:port/database -path ./db/migrations up
migrate help # for more info

# TODO(mattes): thinking about adding some custom flag to allow migration within schemas:
-url="postgres://user@host:port/database?schema=name" 
```

## Authors

* Matthias Kadenbach, https://github.com/mattes