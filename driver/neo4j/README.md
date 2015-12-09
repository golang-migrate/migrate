# Neo4j Driver

* Runs migrations in transcations.
  That means that if a migration failes, it will be safely rolled back.

* Stores migration version details with the label ``SchemaMigrations``.
  An unique constraint for the field :SchemaMigrations(version) will be auto-generated.

* Neo4j cannot perform schema and data updates in a transaction, therefore it's necessary to use different migration files

## Usage

```bash
migrate -url neo4j://user:password@host:port/db/data -path ./db/migrations create add_field_to_table
migrate -url neo4j://user:password@host:port/db/data -path ./db/migrations up
migrate help # for more info
```
## Author

* Carlos Forero, https://github.com/carlosforero