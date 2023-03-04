# Nebula

`nebula://host:port?username=username&password=password&space=topology&x-migrations-tag=schema_migrations&x-timeout=10`

| URL Query  | Description |
|------------|-------------|
| `x-timeout`| Timeout to wait after first tag creation in seconds (default 10s) |
| `x-migrations-tag`| Name of the migrations tag (default is `schema_migrations`) |
| `space` | The name of the space to connect to (required) |
| `username` | The user to sign in as (required) |
| `password` | The user's password (required) |
| `host` | The host to connect to (required) |
| `port` | The port to bind to (required) |


## Notes

* The Nebula driver natively supports executing multipe statements in a single query. But the queries are not executed in any sort of transaction/batch, meaning you are responsible for fixing partial migrations.
* The `space` option is required parameter due to the lack of default Space in Nebula. You are responsible for creating Space before executing migrations
* As for Nebula v3.4.0 you can create Space with VIDs in two formats - `FIXED_STRING(<N>)` or `INT[64]`. This implementation of migration library checks the parameters of the provided `space`. In order to work properly, if you use `FIXED_STRING(<N>)` for VIDs, specified N must be at least 16 bytes. This is because we use time.Now().UnixMicro() for versioning
* If your `space` is empty - creation of the first tag (used for migrations) takes some time. By default timeout is set for 10 seconds. You can specify another value using `x-timeout` option
