# oracle
The golang oracle driver (go-oci8) does not natively support executing multiple statements in a single query. 
Here are the strategies for splitting the migration text into separately-executed statements:
1. If there is no PL/SQL statement in a migration file, the `semicolons` will be the separator
1. If there is any PL/SQL statement in a migration file, the separator will be `---` in a single line or specified by `x-plsql-line-separator`, 
   And in this case the multiple statements cannot be used when a statement in the migration contains a string with the given `line separator`.

`oracle://user/password@host:port/sid?query` (`oci8` works too)

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table in UPPER case |
| `x-plsql-line-separator` | `PLSQLStatementSeparator` | a single line which use as the token to spilt multiple statements in single migration file (See note above), default `---` |

## Building

You'll need to [Install Oracle full client or Instant Client:](https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html) for oracle support since this uses [github.com/mattn/go-oci8](https://github.com/mattn/go-oci8)

## Supported & tested version
- 12c-ee
- 18c-xe

## How to use

In order to compile & run the migration against Oracle database, basically it will require:

## Compile
1. Download [oracle client dynamic library](https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html) from their official site manually, because there is a check box on download page need to honor manually.
1. Build cli 
```bash
$ cd /path/to/repo/dir
$ PKG_CONFIG_PATH=/path/to/oracle/sdk/dir LD_LIBRARY_PATH=/path/to/oracle/lib/dir go build  -tags 'oracle' -o bin/migrate github.com/golang-migrate/migrate/v4/cli
```

## Configure Oracle database
1. Example Oracle version: `Oracle Database Express Edition`, check [here](https://docs.oracle.com/cd/B28359_01/license.111/b28287/editions.htm#DBLIC119) from version details.
1. Start a oracle docker container, using customized oracle-xe image(include a PDB database & default user `oracle` in it)
```bash
$ docker run --name oracle -d -p 1521:1521 -p 5500:5500 --volume ~/data/oracle-xe:/opt/oracle/oradata maxnilz/oracle-xe:18c
```
1. Wait a moment, first time will take a a while to run for as the oracle-xe configure script needs to complete
```

## Play
1. Run test code 
```bash
$ cd /path/to/repo/database/oracle/dir
$ ORACLE_DSN=oracle://oracle/oracle@localhost:1521/XEPDB1 PKG_CONFIG_PATH=/path/to/oracle/lib/dir LD_LIBRARY_PATH=/path/to/oracle/lib/dir go test -race -v -covermode atomic ./... -coverprofile .coverage
```
1. Check [example migration files](examples)

## FAQ
1. Why we need the dynamic library?

Because there is no static lib for the application to compile & link. check [here](https://community.oracle.com/thread/4177571) for more details.

