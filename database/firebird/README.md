# firebird

`firebirdsql://user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as |
| `password` | | The user's password | 
| `host` | | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost) |
| `port` | | The port to bind to. (default is 3050) |
| `auth_plugin_name` | | Authentication plugin name. Srp256/Srp/Legacy_Auth are available. (default is Srp) |
| `column_name_to_lower` | | Force column name to lower. (default is false) |
| `role` | | Role name |
| `tzname` | | Time Zone name. (For Firebird 4.0+) |
| `wire_crypt` | | Enable wire data encryption or not. For Firebird 3.0+ (default is true) |
