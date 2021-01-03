# Vertica


`vertica://user:password@tcp(host:port)/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as |
| `password` | | The user's password | 
| `host` | | The host to connect to. |
| `port` | | The port to bind to. |
| `x-schema` | `Schema` | Optional, schema name. |


## Existing Clients 
If you use the Vertica driver with existing database client, you must create the client with parameter `use_prepared_statements=0`:



```go
package main

import (
    "database/sql"
    
    _ "github.com/vertica/vertica-sql-go"
    "github.com/golang-migrate/migrate"
    "github.com/golang-migrate/migrate/database/vertica"
    _ "github.com/golang-migrate/migrate/source/file"
)

func main() {
    db, _ := sql.Open("vertica", "user:password@tcp(host:port)/dbname?x-schema=public")
    driver, _ := vertica.WithInstance(db, &vertica.Config{})
    m, _ := migrate.NewWithDatabaseInstance(
        "file:///migrations",
        "vertica", 
        driver,
    )
    
    m.Steps(2)
}
```