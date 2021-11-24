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
| `useMsi` | | `true` - Use Azure MSI Authentication for connecting to Sql Server. Must be running from an Azure VM/an instance with MSI enabled. `false` - Use password authentication (Default). See [here for Azure MSI Auth details](https://docs.microsoft.com/en-us/azure/app-service/app-service-web-tutorial-connect-msi). NOTE: Since this cannot be tested locally, this is not officially supported.

See https://github.com/denisenkom/go-mssqldb for full parameter list.

## Driver Support

### Which go-mssqldb driver to us?

Please note that the deprecated `mssql` driver is not supported. Please use the newer `sqlserver` driver.  
See https://github.com/denisenkom/go-mssqldb#deprecated for more information.

### Official Support by migrate

Versions of MS SQL Server 2019 newer than CTP3.1 are not officially supported since there are issues testing against the Docker image.
For more info, see: https://github.com/golang-migrate/migrate/issues/160#issuecomment-522433269
