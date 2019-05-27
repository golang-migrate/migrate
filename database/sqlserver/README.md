# Microsoft SQL Server

`sqlserver://username:password@host/instance?param1=value&param2=value`
`sqlserver://username:password@host:port?param1=value&param2=value`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `username` | |  enter the SQL Server Authentication user id or the Windows Authentication user id in the DOMAIN\User format. On Windows, if user id is empty or missing Single-Sign-On is used. |
| `password` | | The user's password. | 
| `host` | | The host to connect to. |
| `port` | | The port to connect to. |
| `instance` | | SQL Server instance name. |
| `database` | `DatabaseName` | The name of the database to connect to |
| `connection+timeout` | | in seconds (default is 0 for no timeout), set to 0 for no timeout. |
| `dial+timeout` | | in seconds (default is 15), set to 0 for no timeout. |
| `encrypt` | | `disable` - Data send between client and server is not encrypted. `false` - Data sent between client and server is not encrypted beyond the login packet (Default). `true` - Data sent between client and server is encrypted. |
| `app+name` || The application name (default is go-mssqldb). |

See https://github.com/denisenkom/go-mssqldb for full parameter list.

## Note about driver support

Please note that the deprecated `mssql` driver is not supported. Please use the newer `sqlserver` driver.  
See https://github.com/denisenkom/go-mssqldb#deprecated for more information.
