[![Build Status](https://travis-ci.org/mattes/migrate.svg?branch=v3.0-prev)](https://travis-ci.org/mattes/migrate)
[![GoDoc](https://godoc.org/github.com/mattes/migrate?status.svg)](https://godoc.org/github.com/mattes/migrate)
[![Coverage Status](https://coveralls.io/repos/github/mattes/migrate/badge.svg?branch=v3.0-prev)](https://coveralls.io/github/mattes/migrate?branch=v3.0-prev)
[![packagecloud.io](https://img.shields.io/badge/deb-packagecloud.io-844fec.svg)](https://packagecloud.io/mattes/migrate?filter=debs)

# migrate

__Database migrations written in Go. Use as [CLI](#cli-usage) or import as [library](#use-in-your-go-project).__

 * Migrate reads migrations from [sources](#migration-sources) 
   and applies them in correct order to a [database](#databases).
 * Drivers are "dumb", migrate glues everything together and makes sure the logic is bulletproof.  
   (Keeps the drivers lightweight, too.)
 * Database drivers don't assume things or try to correct user input. When in doubt, fail.



## Databases 

Database drivers run migrations. [Add a new database?](database/driver.go)

  * [PostgreSQL](database/postgres)
  * [Cassandra](database/cassandra)
  * [SQLite](database/sqlite)
  * [MySQL/ MariaDB](database/mysql)
  * [Neo4j](database/neo4j)
  * [Ql](database/ql)
  * [MongoDB](database/mongodb)
  * [CrateDB](database/crate)
  * [Shell](database/shell)



## Migration Sources

Source drivers read migrations from local or remote sources. [Add a new source?](source/driver.go)

  * [Filesystem](source/file) - read from fileystem (always included)
  * [Go-Bindata](source/go-bindata) - read from embedded binary data ([jteeuwen/go-bindata](https://github.com/jteeuwen/go-bindata))
  * [Github](source/github) - read from remote Github repositories
  * [AWS S3](source/aws-s3) - read from Amazon Web Services S3
  * [Google Cloud Storage](source/google-cloud-storage) - read from Google Cloud Platform Storage



## CLI usage 

  * Simple wrapper around this library.
  * Handles ctrl+c (SIGINT) gracefully.
  * No config search paths, no config files, no magic ENV var injections.

__[CLI Documentation](cli)__

```
$ brew install migrate --with-postgres 
$ migrate -database postgres://localhost:5432/database up 2
```


## Use in your Go project 

 * API is stable and frozen for this release (v3.x).
 * Package migrate has no external dependencies.
 * Only import the drivers you need.
   (check [dependency_tree.txt](https://github.com/mattes/migrate/releases) for each driver)
 * To help prevent database corruptions, it supports graceful stops via `GracefulStop chan bool`.
 * Bring your own logger.
 * Uses `io.Reader` streams internally for low memory overhead.
 * Thread-safe.

__[Go Documentation](https://godoc.org/github.com/mattes/migrate)__

```go
import (
    "github.com/mattes/migrate/migrate"
    _ "github.com/mattes/migrate/database/postgres"
    _ "github.com/mattes/migrate/source/github"
)

func main() {
    m, err := migrate.New(
        "github://mattes:personal-access-token@mattes/migrate_test",
        "postgres://localhost:5432/database?sslmode=enable")
    m.Steps(2)
}
```

## Migration files

Each migration has an up and down migration. [Why?](FAQ.md#why-two-separate-files-up-and-down-for-a-migration)

```
1481574547_create_users_table.up.sql
1481574547_create_users_table.down.sql
```

[Best practices: How to write migrations.](MIGRATIONS.md)



## Development and Contributing

Yes, please! [`Makefile`](Makefile) is your friend,
read the [development guide](CONTRIBUTING.md).

Also have a look at the [FAQ](FAQ.md).



---

__Alternatives__

https://bitbucket.org/liamstask/goose, https://github.com/tanel/dbmigrate,  
https://github.com/BurntSushi/migration, https://github.com/DavidHuie/gomigrate,  
https://github.com/rubenv/sql-migrate
