# firebird

`firebirdsql://user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `auth_plugin_name` | | Authentication plugin name. Srp256/Srp/Legacy_Auth are available. (default is Srp) |
| `column_name_to_lower` | | Force column name to lower. (default is false) |
| `role` | | Role name |
| `tzname` | | Time Zone name. (For Firebird 4.0+) |
| `wire_crypt` | | Enable wire data encryption or not. For Firebird 3.0+ (default is true) |

## Not officially supported

If you need a `migrate` CLI that supports Firebird, you'll need need to build it yourself. There are also no guarantees that the integration will work since we're not testing with Firebird.

For more info, see: https://github.com/golang-migrate/migrate/issues/49#issuecomment-487193672
