# neo4j

`neo4j://user:password@host:port/`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `user` | Contained within `AuthConfig` | The user to sign in as |
| `password` | Contained within `AuthConfig` | The user's password |
| `host` | | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost) |
| `port` | | The port to bind to. (default is 7687) |
|  | `MigrationsLabel` | Name of the migrations node label |

## Building

You'll need to build [seabolt](https://github.com/neo4j-drivers/seabolt) for neo4j support since this uses [github.com/neo4j/neo4j-go-driver](https://github.com/neo4j/neo4j-go-driver)
