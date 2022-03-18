# Cassandra

* Drop command will not work on Cassandra 2.X because it rely on
system_schema table which comes with 3.X
* Other commands should work properly but are **not tested**
* The Cassandra driver (gocql) does not natively support executing multiple statements in a single query. To allow for multiple statements in a single migration, you can use the `x-multi-statement` param. There are two important caveats:
  * This mode splits the migration text into separately-executed statements by a semi-colon `;`. Thus `x-multi-statement` cannot be used when a statement in the migration contains a string with a semi-colon.
  * The queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.


## Usage
`cassandra://host:port/keyspace?param1=value&param2=value2`


| URL Query  | Default value | Description |
|------------|-------------|-----------|
| `x-migrations-table` | schema_migrations | Name of the migrations table |
| `x-multi-statement` | false | Enable multiple statements to be ran in a single migration (See note above) |
| `port` | 9042 | The port to bind to  |
| `consistency` | ALL | Migration consistency
| `protocol` |  | Cassandra protocol version (3 or 4)
| `timeout` | 1 minute | Migration timeout
| `connect-timeout` | 600ms | Initial connection timeout to the cluster |
| `username` | nil | Username to use when authenticating. |
| `password` | nil | Password to use when authenticating. |
| `sslcert` | | Cert file location. The file must contain PEM encoded data. |
| `sslkey` | | Key file location. The file must contain PEM encoded data. |
| `sslrootcert` | | The location of the root certificate file. The file must contain PEM encoded data. |
| `sslmode` | | Whether or not to use SSL (disable\|require\|verify-ca\|verify-full) |
| `disable-host-lookup`| false | Disable initial host lookup. |

`timeout` is parsed using [time.ParseDuration(s string)](https://golang.org/pkg/time/#ParseDuration)


## Upgrading from v1

1. Write down the current migration version from schema_migrations
2. `DROP TABLE schema_migrations`
4. Download and install the latest migrate version.
5. Force the current migration version with `migrate force <current_version>`.
