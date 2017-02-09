# migrate

[![Build Status](https://travis-ci.org/mattes/migrate.svg?branch=v3.0-prev)](https://travis-ci.org/mattes/migrate)
[![GoDoc](https://godoc.org/github.com/mattes/migrate?status.svg)](https://godoc.org/github.com/mattes/migrate)
[![Coverage Status](https://coveralls.io/repos/github/mattes/migrate/badge.svg?branch=v3.0-prev)](https://coveralls.io/github/mattes/migrate?branch=v3.0-prev)

Database migrations written in Go. Use as CLI or import as library.


```
go get -u -tags 'postgres' -o migrate github.com/mattes/migrate/cli

import (
  "github.com/mattes/migrate"
  _ "github.com/mattes/migrate/database/postgres"
)
```

## Databases 

Database drivers are responsible for applying migrations to databases.
Implementing a new database driver is easy. Just implement [database/driver interface](database/driver.go)

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

Source Drivers read migrations from various locations. Implementing a new source driver
is easy. Just implement the [source/driver interface](source/driver.go).

  * [Filesystem](source/file) - read from fileystem (always included)
  * [Go-Bindata](source/go-bindata) - read from embedded binary data ([jteeuwen/go-bindata](https://github.com/jteeuwen/go-bindata))
  * [Github](source/github) - read from remote Github repositories
  * [AWS S3](source/aws-s3) - read from Amazon Web Services S3
  * [Google Cloud Storage](source/google-cloud-storage) - read from Google Cloud Platform Storage


## CLI usage 

```
# dowload, build and install the CLI tool
# -tags takes database and source drivers and will only build those
$ go get -u -tags 'postgres' -o migrate github.com/mattes/migrate/cli

$ migrate -help
Usage: migrate OPTIONS COMMAND [arg...]
       migrate [ -version | -help ]

Options:
  -source      Location of the migrations (driver://url)
  -path        Shorthand for -source=file://path
  -database    Run migrations against this database (driver://url)
  -prefetch N  Number of migrations to load in advance before executing (default 10)
  -verbose     Print verbose logging
  -version     Print version
  -help        Print usage

Commands:
  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop         Drop everyting inside database
  version      Print current migration version


# so let's say you want to run the first two migrations
migrate -database postgres://localhost:5432/database up 2

# if your migrations are hosted on github
migrate -source github://mattes:personal-access-token@mattes/migrate_test \
  -database postgres://localhost:5432/database down 2
```


## Use in your Go project 

```go
import (
  "github.com/mattes/migrate/migrate"
  _ "github.com/mattes/migrate/database/postgres"
  _ "github.com/mattes/migrate/source/github"
)

func main() {
  m, err := migrate.New("github://mattes:personal-access-token@mattes/migrate_test",
    "postgres://localhost:5432/database?sslmode=enable")
  m.Steps(2)
}
```

## Migration files

Each migration version has an up and down migration.

```
1481574547_create_users_table.up.sql
1481574547_create_users_table.down.sql
```

## Development and Testing

Tests require Docker (for database driver testing).

```
make test-short DATABASE='postgres'
make test
```

## Alternatives

 * https://bitbucket.org/liamstask/goose
 * https://github.com/tanel/dbmigrate
 * https://github.com/BurntSushi/migration
 * https://github.com/DavidHuie/gomigrate
 * https://github.com/rubenv/sql-migrate


