# DGraph


`dgraph://[user:password]@host:port?[graphql=true&][cloud=true&][api-key=api-key]`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `user` | | Used for enterprise ACL |
| `password` | | Used for enterprise ACL | 
| `host` | | The host to connect to. |
| `port` | | The port to bind to. (default is 9080) |
| `graphql` | | Should migrate use GraphQL naming conventions (Allows GraphQL schema to be attached) |
| `cloud` | | Is the migration target in cloud? |
| `api-key` | | If the migration target is in the cloud, apply api-key |

## Examples
### Cloud

./migrate -path ./database/dgraph/examples -database "dgraph://some-name.eu-west-1.aws.cloud.dgraph.io/graphql?api-key=MY-SECRET-API-KEY&graphql=true&" up

### Self-Hosted

./migrate -path ./database/dgraph/examples -database "https://some-url:9080?graphql=true&" up

# GraphQL 

GraphQL Support will be added. If you want to access Migration info from GraphQL add the following to your schema:
```
type Migration {
    version: Int
    dirty: Boolean
}
```