# mysql

`mysql://user:password@tcp(host:port)/dbname?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `dbname` | `DatabaseName` | The name of the database to connect to |
| `user` | | The user to sign in as |
| `password` | | The user's password | 
| `host` | | The host to connect to. |
| `port` | | The port to bind to. |
| `x-tls-ca` | | The location of the root certificate file. |
| `x-tls-cert` | | Cert file location. |
| `x-tls-key` | | Key file location. | 
| `x-tls-insecure-skip-verify` | | Whether or not to use SSL (true\|false) | 

