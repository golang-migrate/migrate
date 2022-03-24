# MongoDB

* Driver work with mongo through [db.runCommands](https://docs.mongodb.com/manual/reference/command/)
* Migrations support json format. It contains array of commands for `db.runCommand`. Every command is executed in separate request to database 
* All keys have to be in quotes `"`
* [Examples](./examples)

# Usage

`mongodb://user:password@host:port/dbname?query` (`mongodb+srv://` also works, but behaves a bit differently. See [docs](https://docs.mongodb.com/manual/reference/connection-string/#dns-seedlist-connection-format) for more information)

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