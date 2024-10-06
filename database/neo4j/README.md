# neo4j
The Neo4j driver (bolt) does not natively support executing multiple statements
in a single query. To allow for multiple statements in a single migration, you
can use the `x-multi-statement` param. This mode splits the migration text into
separately-executed statements by a semicolon `;`. Thus `x-multi-statement`
cannot be used when a statement in the migration contains a string with a
semicolon. The queries **should** run in a single transaction, so partial
migrations should not be a concern, but this is untested.

Here are possible connection URLs:

- `neo4j://user:password@host:port/`
- `neo4j+s://user:password@host:port/`
- `neo4j+ssc://user:password@host:port/`
- `bolt://user:password@host:port/`
- `bolt+s://user:password@host:port/`
- `bolt+ssc://user:password@host:port/`

| URL Query           | WithInstance Config           | Description                                                                                          |
|---------------------|-------------------------------|------------------------------------------------------------------------------------------------------|
| `x-multi-statement` | `MultiStatement`              | Enable multiple statements to be ran in a single migration (See note above)                          |
| `user`              | Contained within `AuthConfig` | The user to sign in as                                                                               |
| `password`          | Contained within `AuthConfig` | The user's password                                                                                  |
| `host`              |                               | The host to connect to. Values that start with / are for unix domain sockets. (default is localhost) |
| `port`              |                               | The port to bind to. (default is 7687)                                                               |
|                     | `MigrationsLabel`             | Name of the migrations node label                                                                    |


## Supported versions

Neo4j v4.4 LTS and v5+ is supported.

Make sure to check [End Of Life dates](https://neo4j.com/developer/kb/neo4j-supported-versions/) of Neo4j versions.