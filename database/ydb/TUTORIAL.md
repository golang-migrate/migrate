# YDB tutorial for beginners

## Start a local YDB instance

The easiest way to get started with YDB locally is using Docker:
```bash
docker run -d --name ydb-local \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e GRPC_PORT=2136 \
  -e MON_PORT=8765 \
  -p 2136:2136 \
  -p 8765:8765 \
  cr.yandex/yc/yandex-docker-local-ydb:latest
```

When using Migrate CLI we need to pass the database URL. Let's export it to a variable for convenience:
```bash
export YDB_URL='grpc://localhost:2136/local'
```

You can find further description of database URLs [here](README.md).

## Create migrations

Let's create a table called `users`:
```bash
migrate create -ext sql -dir db/migrations -seq create_users_table
```
If there were no errors, we should have two files available under `db/migrations` folder:
- 000001_create_users_table.down.sql
- 000001_create_users_table.up.sql

Note the `sql` extension that we provided.

In the `.up.sql` file let's create the table:
```sql
CREATE TABLE users (
    user_id Int64 NOT NULL,
    username Utf8,
    password Utf8,
    email Utf8,
    PRIMARY KEY (user_id)
);
```

**Important YDB differences from SQL:**
- Every table must have a `PRIMARY KEY` clause.
- YDB uses its own type system: `Int64`, `Uint64`, `Utf8`, `Bool`, `Date`, `Timestamp`, etc.
- There is no `serial`/`autoincrement` — you must manage IDs yourself.
- `IF NOT EXISTS` / `IF EXISTS` is not supported for `CREATE TABLE` / `DROP TABLE`.

And in the `.down.sql` let's delete it:
```sql
DROP TABLE users;
```

## Run migrations
```bash
migrate -database ${YDB_URL} -path db/migrations up
```

Let's check if the table was created properly using the YDB CLI or the monitoring UI at `http://localhost:8765`.

To run reverse migration:
```bash
migrate -database ${YDB_URL} -path db/migrations down
```

## Multi-statement migrations

If you need to run multiple DDL statements in a single migration file (e.g., create a table and an index),
you must enable multi-statement mode by adding `x-multi-statement=true` to the URL:
```bash
export YDB_URL='grpc://localhost:2136/local?x-multi-statement=true'
```

Example migration with multiple statements:
```sql
CREATE TABLE orders (
    order_id Int64 NOT NULL,
    user_id Int64 NOT NULL,
    amount Uint64,
    PRIMARY KEY (order_id)
);
ALTER TABLE orders ADD INDEX idx_orders_user GLOBAL ON (user_id);
```

Each statement is delimited by `;` and executed separately.

## Optional: Run migrations within your Go app

Here is a very simple app running migrations for the above configuration:
```go
package main

import (
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/ydb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	m, err := migrate.New(
		"file://db/migrations",
		"grpc://localhost:2136/local")
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil {
		log.Fatal(err)
	}
}
```

You can find more details [here](README.md).

