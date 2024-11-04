# [YDB](https://ydb.tech/docs/)

`ydb://[user:password@]host:port/database?QUERY_PARAMS`

| URL Query  |               Description               |
|:----------:|:---------------------------------------:|
|   `user`   |         The user to sign in as.         |
| `password` |          The user's password.           | 
|   `host`   |         The host to connect to.         |
|   `port`   |          The port to bind to.           |                                     |
| `database` | The name of the database to connect to. |

|   URL Query Params   |                         Description                         |
|:--------------------:|:-----------------------------------------------------------:|
|    `x-auth-token`    |                    Authentication token.                    |
| `x-migrations-table` | Name of the migrations table (default `schema_migrations`). |
|    `x-use-grpcs`     | Enables gRPCS protocol for YDB connections (default grpc).  |

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