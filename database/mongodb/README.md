# MongoDB

* Driver work with mongo through [db.runCommands](https://docs.mongodb.com/manual/reference/command/)
* Migrations support json format. It contains array of commands for `db.runCommand`. Every command is executed in separate request to database 
* All keys have to be in quotes `"`
* [Examples](./examples)

# Usage

`mongodb://user:password@host:port/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-collection` | `MigrationsCollection` | Name of the migrations collection |
| `x-transaction-mode` | `TransactionMode` | If set to `true` wrap commands in [transaction](https://docs.mongodb.com/manual/core/transactions). Available only for replica set. Driver is using [strconv.ParseBool](https://golang.org/pkg/strconv/#ParseBool) for parsing|
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as. Can be omitted |
| `password` | | The user's password. Can be omitted | 
| `host` | | The host to connect to |
| `port` | | The port to bind to |