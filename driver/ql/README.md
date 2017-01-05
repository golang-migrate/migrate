# Ql Driver

* Supports both in-memory and file ql databases, with the URL schemes `ql+file://` and `ql+memory://`
    * In-memory driver is not of much use, since the database is destroyed once the migration operation finishes, but is included for completeness.
* Stores migration version details in automatically generated table ``schema_migrations``.

## Usage

```bash
migrate -url ql+file://./data.db -path ./migrations create add_field_to_table
migrate -url ql+file://./data.db -path ./db/migrations up
migrate help # for more info
```

## Authors

* Sam Willcocks, https://github.com/wlcx
