# Amazon RDS (Aurora Serverless)

`rds://?resource_arn=...&secret_arn=...&database=...&aws_region=...`

| URL Query                    | WithInstance Config     | Database | Description                                                                                                                                                                                                                                                                  |
|------------------------------|-------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `resource_arn`               | `ResourceARN`           |          | ARN of the RDS instance.                                                                                                                                                                                                                                                        |
| `secret_arn`                 | `SecretARN`             |          | ARN of the SecretManager secret which stores the database credentials.                                                                                                                                                                                                       |
| `database`                   | `Database`              |          | Name of the database.                                                                                                                                                                                                                                                        |
| `aws_region`                 | `AWSRegion`             |          | AWS Region in which the RDS instance exists.                                                                                                                                                                                                                                 |
| `x-migrations-table`         | `MigrationsTable`       |          | Name of the migrations table                                                                                                                                                                                                                                                 |
| `x-no-lock`                  | `NoLock`                | MySQL    | Set to `true` to skip `GET_LOCK`/`RELEASE_LOCK` statements. Useful for [multi-master MySQL flavors](https://www.percona.com/doc/percona-xtradb-cluster/LATEST/features/pxc-strict-mode.html#explicit-table-locking). Only run migrations from one host when this is enabled. |
| `x-migrations-table-quoted` | `MigrationsTableQuoted` | Postgres | By default, migrate quotes the migration table for SQL injection safety reasons. This option disable quoting and naively checks that you have quoted the migration table name. e.g. `"my_schema"."schema_migrations"`                                                        |
| `x-statement-timeout` | `StatementTimeout`      | Postgres | Abort any statement that takes more than the specified number of milliseconds                                                                                                                                                                                                |
| `x-multi-statement` | `MultiStatementEnabled` | Postgres | Enable multi-statement execution (default: false)                                                                                                                                                                                                                            |
| `x-multi-statement-max-size` | `MultiStatementMaxSize` | Postgres | Maximum size of single statement in bytes (default: 10MB)                                                                                                                                                                                                                    |

## 

```go
package main

import (
	"database/sql"
	"github.com/golang-migrate/migrate/v4"
	rdsdriver "github.com/krotscheck/go-rds-driver"
	"github.com/golang-migrate/migrate/v4/database/rds"
)

func main() {
	config := rdsdriver.NewConfig("resource_arn", "secret_arn", "database_name", "aws_region")
	dsn := config.ToDSN()
	db, _ := sql.Open(rdsdriver.DRIVERNAME, dsn)
	driver, _ := rds.WithInstance(db, config)
	m, _ := migrate.NewWithDatabaseInstance("file:///migrations", "rds", driver)

	m.Steps(2)
}
```

## Implementation details

RDS's data api is an HTTP wrapper on top of a normal MySQL or Postgres database. As such,
this migration driver is little more than a factory, which constructs an sql.DB instance
using the RDS driver, and then wraps it in the appropriate database migrator. In other words,
while you may invoke `WithInstance` or `rds.Open()` on the rds package, you'll actually receive
an instance of `mysql.Mysql` or `postgres.Postgres`.

For details on DSN options and how to construct an instance, please see https://github.com/krotscheck/go-rds-driver
