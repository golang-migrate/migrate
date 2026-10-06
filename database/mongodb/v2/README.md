# MongoDB

This package uses [MongoDB Go driver v2](https://pkg.go.dev/go.mongodb.org/mongo-driver/v2). A backend for [v1](..) is also available.

* Driver work with mongo through [db.runCommands](https://docs.mongodb.com/manual/reference/command/)
* Migrations support json format. It contains array of commands for `db.runCommand`. Every command is executed in separate request to database
* All keys have to be in quotes `"`
* [Examples](../examples)

# Usage

Import `github.com/golang-migrate/migrate/v4/database/mongodb/v2` to register this driver. For CLI builds, use the `mongodb2` build tag.

`mongodb2://user:password@host:port/dbname?query` (`mongodb2+srv://` also works, but behaves a bit differently. See [docs](https://docs.mongodb.com/manual/reference/connection-string/#dns-seedlist-connection-format) for more information)

`WithInstance` accepts a `*mongo.Client` from `go.mongodb.org/mongo-driver/v2/mongo`. Construct that client with a standard `mongodb://` or `mongodb+srv://` URI.

This backend retains the JSON command format and default migration and lock collection names used by v1.

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-collection` | `MigrationsCollection` | Name of the migrations collection |
| `x-transaction-mode` | `TransactionMode` | If set to `true` wrap commands in [transaction](https://docs.mongodb.com/manual/core/transactions). Available only for replica set. Driver is using [strconv.ParseBool](https://golang.org/pkg/strconv/#ParseBool) for parsing|
| `x-advisory-locking` | `true` | Feature flag for advisory locking, if set to false, disable advisory locking |
| `x-advisory-lock-collection` | `migrate_advisory_lock` | The name of the collection to use for advisory locking.|
| `x-advisory-lock-timeout` | `15` | The max time in seconds that migrate will wait to acquire a lock before failing. |
| `x-advisory-lock-timeout-interval` | `10` | The max time in seconds between attempts to acquire the advisory lock, the lock is attempted to be acquired using an exponential backoff algorithm. |
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as. Can be omitted |
| `password` | | The user's password. Can be omitted |
| `host` | | The host to connect to |
| `port` | | The port to bind to |
