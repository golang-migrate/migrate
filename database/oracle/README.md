# oracle
The golang oracle driver (go-oci8) does not natively support executing multiple statements in a single query. 
To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. 
This mode splits the migration text into separately-executed statements:
1. If there is no PL/SQL statement in a migration file, the `semicolons` will be the separator
2. If there is any PL/SQL statement in a migration file, the separator will be `---` in a single line or specified by `x-plsql-line-separator`, 
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
2. Build cli: `LD_LIBRARY_PATH=/path/to/lib/dir go build  -tags 'oracle' -o bin/migrate github.com/golang-migrate/migrate/v4/cli`

## Configure Oracle database
1. Example Oracle version: `Oracle Database Express Edition`, check [here](https://docs.oracle.com/cd/B28359_01/license.111/b28287/editions.htm#DBLIC119) from version details.
2. Start a oracle docker container, e.g, `docker run --name oracle -d -p 1521:1521 -p 5500:5500 --volume ~/data/oracle-xe:/opt/oracle/oradata maxnilz/oracle-xe:18c`
3. Wait a moment for the first startup.
4. Connect to oracle server via [sqlpus](https://download.oracle.com/otn/linux/instantclient/185000/instantclient-sqlplus-linux.x64-18.5.0.0.0dbru.zip) by using the builtin sys user & password, 
   create a user in Oracle PDB container.
```bash
$ sqlplus sys/Oracle18@localhost:1521/XE as sysdba << EOF
  show con_name
  show pdbs  

  alter session set container=XEPDB1;
  create user oracle identified by oracle;
  grant dba to oracle;
  grant create session to oracle;
  grant connect, resource to oracle;
  grant all privileges to oracle;
  
  exit;
EOF
```

## Play
1. Run test code 
```bash
$ cd /path/to/database/oracle
$ ORACLE_DSN=oracle://oracle/oracle@localhost:1522/XEPDB1 PKG_CONFIG_PATH=/opt/oracle/instantclient_18_5 LD_LIBRARY_PATH=/opt/oracle/instantclient_18_5 go test -race -v -covermode atomic ./... -coverprofile .coverage
```
2. Check [example migration files](examples)

## FAQ
1. Why we need the dynamic library?
Because there is no static lib for the application to compile & link. check [here](https://community.oracle.com/thread/4177571) for more details.
 
