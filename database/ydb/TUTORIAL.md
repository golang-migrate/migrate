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

When using the Migrate CLI we need to pass the database URL. Let's export it to a variable for convenience:
```bash
export YDB_URL='grpc://localhost:2136/local'
```

You can find further description of database URLs and all supported parameters in [README.md](README.md).

## Create migrations

Let's create a table called `users`:
```bash
migrate create -ext sql -dir db/migrations -seq create_users_table
```

If there were no errors, we should have two files available under `db/migrations/`:
- `000001_create_users_table.up.sql`
- `000001_create_users_table.down.sql`

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

And in the `.down.sql` let's delete it:
```sql
DROP TABLE users;
```

## Run migrations

```bash
migrate -database "${YDB_URL}" -path db/migrations up
```

Check that the table was created using the YDB CLI or the monitoring UI at `http://localhost:8765`.

To run the reverse migration:
```bash
migrate -database "${YDB_URL}" -path db/migrations down
```

## Run migrations within your Go app

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

For TLS connections, multi-statement migrations, and all other configuration options see [README.md](README.md).

