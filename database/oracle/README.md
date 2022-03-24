# oracle

The supported oracle specific options can be configured in the query section of the oracle URL `oracle://user:password@host:port/sid?query`

| URL Query                | WithInstance Config  | Description                                                                                                             |
|--------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------|
| `x-migrations-table`     | `MigrationsTable`    | Name of the migrations table in UPPER case                                                                              |
| `x-multi-stmt-enabled`   | `MultiStmtEnabled`   | If the migration files are in multi-statements style                                                                    |
| `x-multi-stmt-separator` | `MultiStmtSeparator` | a single line which use as the token to spilt multiple statements in single migration file, triple-dash separator `---` |

## Run-time Requirements
- Oracle Client libraries - see [ODPI-C](https://oracle.github.io/odpi/doc/installation.html)

Although Oracle Client libraries are NOT required for compiling, they *are*
needed at run time.  Download the free Basic or Basic Light package from
<https://www.oracle.com/database/technologies/instant-client/downloads.html>.

```shell
sudo apt-get install -y libaio1 wget unzip
wget -O /tmp/instantclient-basic-linux-x64.zip https://download.oracle.com/otn_software/linux/instantclient/193000/instantclient-basic-linux.x64-19.3.0.0.0dbru.zip
mkdir -p /usr/lib/oracle && unzip /tmp/instantclient-basic-linux-x64.zip -d /usr/lib/oracle
ldconfig -v /usr/lib/oracle/instantclient_19_3
ldd /usr/lib/oracle/instantclient_19_3/libclntsh.so
```

## Write migration files

There are two ways to write the migration files,

1. Single statement file in which it contains only one SQL statement or one PL/SQL statement(Default)
2. Multi statements file in which it can have multi statements(can be SQL or PL/SQL or mixed)

### Single statement file

Oracle godor driver support process one statement at a time, so it is natural to support single statement per file as the default.
Check the [single statement migration files](examples/migrations) as an example.

### Multi statements file

Although the golang oracle driver [godror](https://github.com/godror/godror) does not natively support executing multiple
statements in a single query, it's more friendly and handy to support multi statements in a single migration file in some case,
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

Start the `Oracle Database Instance` via docker first
```
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

```bash
$ cd /path/to/repo/database/oracle/dir
$ ORACLE_DSN=oracle://sys:oracle@localhost:1521/XEPDB1 go test -tags "oracle" -race -v -covermode atomic ./... -coverprofile .coverage
```
