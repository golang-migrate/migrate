# migrate

[![Build Status](https://travis-ci.org/mattes/migrate.svg?branch=v3.0-prev)](https://travis-ci.org/mattes/migrate)
[![GoDoc](https://godoc.org/github.com/mattes/migrate?status.svg)](https://godoc.org/github.com/mattes/migrate)
[![Coverage Status](https://coveralls.io/repos/github/mattes/migrate/badge.svg?branch=v3.0-prev)](https://coveralls.io/github/mattes/migrate?branch=v3.0-prev)
[![packagecloud.io](https://img.shields.io/badge/deb-packagecloud.io-844fec.svg)](https://packagecloud.io/mattes/migrate?filter=debs)

__Database migrations written in Go. Use as CLI or import as library.__


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

__[CLI Documentation](cli/README.md)__


Example:

```
go get -u -tags 'postgres' -o migrate github.com/mattes/migrate/cli

migrate -database postgres://localhost:5432/database up 2
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

## Development, Testing and Contributing

__[Guide](CONTRIBUTING.md)__


## Alternatives

 * https://bitbucket.org/liamstask/goose
 * https://github.com/tanel/dbmigrate
 * https://github.com/BurntSushi/migration
 * https://github.com/DavidHuie/gomigrate
 * https://github.com/rubenv/sql-migrate


