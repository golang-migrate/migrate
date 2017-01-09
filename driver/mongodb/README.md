# MongoDB Driver

* Runs pre-registered Golang methods that receive a single `*mgo.Session` parameter and return `error` on failure.
* Stores migration version details in collection ``db_migrations``.
  This collection will be auto-generated.
* Migrations do not run in transactions, there are no built-in transactions in MongoDB.
  That means that if a migration fails, it will not be rolled back.
* There is no out-of-the-box support for command-line interface via terminal.

## Usage in Go

```go
import "github.com/mattes/migrate/migrate"

// Import your migration methods package so that they are registered and available for the MongoDB driver.
// There is no need to import the MongoDB driver explicitly, as it should already be imported by your migration methods package.
import _ "my_mongo_db_migrator"

// use synchronous versions of migration functions ...
allErrors, ok := migrate.UpSync("mongodb://host:port", "./path")
if !ok {
  fmt.Println("Oh no ...")
  // do sth with allErrors slice
}

// use the asynchronous version of migration functions ...
pipe := migrate.NewPipe()
go migrate.Up(pipe, "mongodb://host:port", "./path")
// pipe is basically just a channel
// write your own channel listener. see writePipe() in main.go as an example.
```

## Migration files format

The migration files should have an ".mgo" extension and contain a list of registered methods names.

Migration methods should satisfy the following:
* They should be exported (their name should start with a capital letter) 
* Their type should be `func (*mgo.Session) error`

Recommended (but not required) naming conventions for migration methods:
* Prefix with V<version> : for example V001 for version 1. 
* Suffix with "_up" or "_down" for up and down migrations correspondingly.

001_first_release.up.mgo
```
V001_some_migration_operation_up
V001_some_other_operation_up
...
```

001_first_release.down.mgo
```
V001_some_other_operation_down
V001_some_migration_operation_down
...
```


## Methods registration

For a detailed example see: [sample_mongodb_migrator.go](https://github.com/mattes/migrate/blob/master/driver/mongodb/example/sample_mongdb_migrator.go)

```go
package my_mongo_db_migrator

import (
  "github.com/mattes/migrate/driver/mongodb"
  "github.com/mattes/migrate/driver/mongodb/gomethods"
  "gopkg.in/mgo.v2"
)

// common boilerplate
type MyMongoDbMigrator struct {
}

func (r *MyMongoDbMigrator) DbName() string {
  return "<target_db_name_for_migration>"
}

var _ mongodb.MethodsReceiver = (*MyMongoDbMigrator)(nil)

func init() {
  gomethods.RegisterMethodsReceiverForDriver("mongodb", &MyMongoDbMigrator{})
}


// Here goes the application-specific migration logic
func (r *MyMongoDbMigrator) V001_some_migration_operation_up(session *mgo.Session) error {
  // do something
  return nil
}

func (r *MyMongoDbMigrator) V001_some_migration_operation_down(session *mgo.Session) error {
  // revert some_migration_operation_up from above
  return nil
}

```

## Authors

* Demitry Gershovich, https://github.com/dimag-jfrog

