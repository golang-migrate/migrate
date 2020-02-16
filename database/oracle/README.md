# oracle
The golang oracle driver (go-oci8) does not natively support executing multiple statements in a single query. 
Here are the strategies for splitting the migration text into separately-executed statements:
1. If there is no PL/SQL statement in a migration file, the `semicolons` will be the separator
1. If there is any PL/SQL statement in a migration file, the separator will be `---` in a single line or specified by `x-plsql-line-separator`, 
   And in this case the multiple statements cannot be used when a statement in the migration contains a string with the given `line separator`.

`oracle://user:password@host:port/sid?query`

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

## Compile & link
1. Download [oracle client dynamic library](https://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html) from their official site manually, because it requires to logon and honor a check box on download page manually.
1. Build cli 
```bash
$ cd /path/to/repo/dir
$ go build  -tags 'oracle' -o bin/migrate github.com/golang-migrate/migrate/v4/cli
```

## Configure Oracle database
1. Example Oracle version: `Oracle Database Express Edition`, check [here](https://docs.oracle.com/cd/B28359_01/license.111/b28287/editions.htm#DBLIC119) from version details.
1. Start a oracle docker container based on customized community oracle-xe image(include a PDB database & default user `oracle` in it): `docker run --name oracle -d -p 1521:1521 -p 5500:5500 --volume ~/data/oracle-xe:/opt/oracle/oradata maxnilz/oracle-xe:18c`
1. Wait a moment, first time will take a while to run for as the oracle-xe configure script needs to complete

## Play

### Run test code 

```bash
$ cd /path/to/repo/database/oracle/dir
$ ORACLE_DSN=oracle://oracle:oracle@localhost:1521/XEPDB1 LD_LIBRARY_PATH=/path/to/oracle/lib/dir go test -tags "oracle" -race -v -covermode atomic ./... -coverprofile .coverage
```

### Write migration files

Check [example migration files](examples)

## FAQs

Maybe not "frequently asked", but hopefully these answers will be useful.

### Why the test code for oracle in CI are disabled

Setup test case via oracle container in CI is very expensive for these reasons:
1. There is no public official docker images available
1. The oracle image size in community is 8GB, which is huge
1. The volume size of one single oracle container is about 5GB, which is huge too
1. And more importantly, It will take a long time to start just a single oracle container & configure it(almost 30min on my 16GB memory, 8 cores machine). The test case will run in parallel and each case will require it's own container, which will increase the resource & time costs many times.
1. Although an Oracle client is NOT required for compiling, it is at run time. and it's tricky to download the dynamic lib directly/automatically because of the oracle download policies. 

### Why there is a dockerfile for oracle only?

The dependent dynamic libs are missing in alpine system, the dockerfile for oracle is based on debian system.

### Why there is an assets dir for the oracle sdk & libs

1. It requires to login to the oracle official site & config the license manually for downloading these oracle sdk & lib, we can't use wget & curl to download directly.
1. In order to make `Dockerfile.oracle` works, I download them manually and put them in the `assets` dir.

### Why we need the dynamic library?

There is no static lib for the application to compile & link. check [here](https://community.oracle.com/thread/4177571) for more details.
