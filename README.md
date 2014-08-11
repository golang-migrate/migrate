# migrate

[![Build Status](https://travis-ci.org/mattes/migrate.svg?branch=master)](https://travis-ci.org/mattes/migrate)
[![GoDoc](https://godoc.org/github.com/mattes/migrate?status.svg)](https://godoc.org/github.com/mattes/migrate)


migrate can be used as CLI or can be imported into your existing Go code.


## Available Drivers

 * [Postgres](https://github.com/mattes/migrate/tree/master/driver/postgres)
 * Bash (planned)

Need another driver? Just implement the [Driver interface](http://godoc.org/github.com/mattes/migrate/driver#Driver) and open a PR.


## Usage from Terminal

```bash
# install
go get github.com/mattes/migrate

# create new migration
migrate -url postgres://user@host:port/database -path ./db/migrations create add_field_to_table

# apply all *up* migrations
migrate -url postgres://user@host:port/database -path ./db/migrations up

# apply all *down* migrations
migrate -url postgres://user@host:port/database -path ./db/migrations down

# roll back the most recently applied migration, then run it again.
migrate -url postgres://user@host:port/database -path ./db/migrations redo

# down and up again
migrate -url postgres://user@host:port/database -path ./db/migrations reset

# show current migration version
migrate -url postgres://user@host:port/database -path ./db/migrations version

# apply the next n migrations
migrate -url postgres://user@host:port/database -path ./db/migrations migrate +1
migrate -url postgres://user@host:port/database -path ./db/migrations migrate +2
migrate -url postgres://user@host:port/database -path ./db/migrations migrate +n

# apply the *down* migration of the current version 
# and the previous n-1 migrations
migrate -url postgres://user@host:port/database -path ./db/migrations migrate -1
migrate -url postgres://user@host:port/database -path ./db/migrations migrate -2
migrate -url postgres://user@host:port/database -path ./db/migrations migrate -n
```


## Usage from within Go

See http://godoc.org/github.com/mattes/migrate/migrate

```golang
import "github.com/mattes/migrate/migrate"


migrate.Up("postgres://user@host:port/database", "./db/migrations")
// ... 
// ... 
```

## Migrations format

```
./db/migrations/001_initial.up.sql
./db/migrations/001_initial.down.sql
```

Why two files? This way you could do sth like ``psql -f ./db/migrations/001_initial.up.sql``.


## Credits

 * https://bitbucket.org/liamstask/goose