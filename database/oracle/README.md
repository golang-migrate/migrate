# oracle

The supported oracle specific options can be configured in the query section of the oracle
URL `oracle://user:password@host:port/ServiceName?query`

| URL Query                | WithInstance Config  | Description                                                                                                             |
|--------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------|
| `x-migrations-table`     | `MigrationsTable`    | Name of the migrations table in UPPER case                                                                              |
| `x-multi-stmt-enabled`   | `MultiStmtEnabled`   | If the migration files are in multi-statements style                                                                    |
| `x-multi-stmt-separator` | `MultiStmtSeparator` | a single line which use as the token to spilt multiple statements in single migration file, triple-dash separator `---` |

## Write migration files

There are two ways to write the migration files,

1. Single statement file in which it contains only one SQL statement or one PL/SQL statement(Default)
2. Multi statements file in which it can have multi statements(can be SQL or PL/SQL or mixed)

### Single statement file

Oracle godor driver support process one statement at a time, so it is natural to support single statement per file as
the default.
Check the [single statement migration files](examples/migrations) as an example.

### Multi statements file

Although the golang oracle driver [godror](https://github.com/godror/godror) does not natively support executing
multiple
statements in a single query, it's more friendly and handy to support multi statements in a single migration file in
some case,
so the multi statements can be separated with a line separator(default to triple-dash separator ---), for example:

```
statement 1
---
statement 2
```

Check the [multi statements' migration files](examples/migrations-multistmt) as an example.

## Supported & tested version

- 18-xe

## Build cli

```bash
$ cd /path/to/repo/dir
$ go build  -tags 'oracle' -o bin/migrate github.com/golang-migrate/migrate/v4/cli
```

## Run test code

There are two ways to run the test code:

- Run the test code locally with an existing Oracle Instance(Recommended)
- Run the test code inside a container just like CI, It will require to start an Oracle container every time, and it's
  very time expense.

### Run the test code locally with an existing Oracle Instance

1. Start the `Oracle Database Instance` via docker first, so that you can reuse whenever you want to run the test code.

```bash
$ cat docker-compose.yaml
---
version: '2'
services:
  orclxe:
    image: container-registry.oracle.com/database/express:18.4.0-xe
    ports:
      - 1521:1521
    container_name: orclxe
    environment:
      ORACLE_PWD: oracle
    volumes:
      - ${HOME}/data/orclxe:/opt/oracle/oradata  # permission chown -R 54321:54321 ${HOME}/data/orclxe

```

2. Go into the sqlplus console

```bash
$ docker exec -it orclxe bash
# su oracle
$ sqlplus / as sysdba
```

3. Create a test DB

```sql
alter session set container=XEPDB1;
create user orcl identified by orcl;
grant dba to orcl;
grant create session to orcl;
grant connect, resource to orcl;
grant all privileges to orcl;
```

4. Run the test code

```bash
$ cd /path/to/repo/database/oracle/dir
$ ORACLE_DSN=oracle://orcl:orcl@localhost:1521/XEPDB1 go test -tags "oracle" -race -v -covermode atomic ./... -coverprofile .coverage  -timeout 20m
```
