# [YDB](https://ydb.tech/docs/)

`ydb://[user:password@]host:port/database?QUERY_PARAMS`

| URL Query  |               Description               |
|:----------:|:---------------------------------------:|
|   `user`   |         The user to sign in as.         |
| `password` |          The user's password.           | 
|   `host`   |         The host to connect to.         |
|   `port`   |          The port to bind to.           |                                     
| `database` | The name of the database to connect to. |

|       URL Query Params       |                                                        Description                                                         |
|:----------------------------:|:--------------------------------------------------------------------------------------------------------------------------:|
|        `x-auth-token`        |                                                   Authentication token.                                                    |
|     `x-migrations-table`     |                                Name of the migrations table (default `schema_migrations`).                                 |
|        `x-lock-table`        |                       Name of the table which maintains the migration lock (default `schema_lock`).                        |
|        `x-force-lock`        | Enables force lock acquisition to fix faulty migrations which may not have released the schema lock (disabled by default). |
|        `x-use-grpcs`         |                                 Enables gRPCS protocol for YDB connections (default grpc).                                 |
|          `x-tls-ca`          |                                    The location of the CA (certificate authority) file.                                    |
| `x-tls-insecure-skip-verify` |                      Controls whether a client verifies the server's certificate chain and host name.                      |
|     `x-tls-min-version`      |                Controls the minimum TLS version that is acceptable, use 1.0, 1.1, 1.2 or 1.3 (default 1.2).                |

### Secure connection

Query param `x-use-grpcs` enables secure TLS connection that requires certificates.
You can declare root certificate using ENV
variable: `export YDB_SSL_ROOT_CERTIFICATES_FILE=/path/to/ydb/certs/CA.pem` or
by using `x-tls-ca` query param: `?x-tls-ca=/path/to/ydb/certs/CA.pem`.

### Authentication

By default, golang-migrate connects to YDB
using [anonymous credentials](https://ydb.tech/docs/en/recipes/ydb-sdk/auth-anonymous). \
Through the url query, you can change the default behavior:

- To connect to YDB using [static credentials](https://ydb.tech/docs/en/recipes/ydb-sdk/auth-static) you need to specify
  username and password:
  `ydb://user:password@host:port/database`
- To connect to YDB using [token](https://ydb.tech/docs/en/recipes/ydb-sdk/auth-access-token) you need to specify token
  as query parameter:
  `ydb://host:port/database?x-auth-token=<YDB_TOKEN>`