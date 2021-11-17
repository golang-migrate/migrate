# DGraph


`dgraph://[user:password]@host:port?[namespace=namespace&][graphql=true&][cloud=true&][api-key=api-key&][ssl=true&][gql-port=port&][graphql-token-header=token-header&][graphql-token-value=token-value]`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `user` | | Used for enterprise ACL |
| `password` | | Used for enterprise ACL | 
| `namespace` | | Used for enterprise ACL | 
| `host` | | The host to connect to. |
| `port` | | The port to bind to. (default is 9080) |
| `graphql` | | Should migrate use GraphQL naming conventions (Allows GraphQL schema to be attached) |
| `cloud` | | Is the migration target in cloud? |
| `api-key` | | If the migration target is in the cloud, apply api-key |
| `ssl` | | Use https to connect to the server |
| `gql-port` | | Port used for the graphql connection |
| `admin-token-header` | | Auth-Header for the admin endpoint connection |
| `admin-token-value` | | Auth-Token for the admin endpoint connection |
| `graphql-token-header` | | Auth-Header for the graphql connection |
| `graphql-token-value` | | Auth-Token for the graphql connection |

## Examples
### Cloud

./migrate -path ./database/dgraph/examples -database "dgraph://some-name.eu-west-1.aws.cloud.dgraph.io/graphql?api-key=MY-SECRET-API-KEY&graphql=true&ssl=true&gql-port=443&admin-token-header=Dg-Auth&admin-token-value=MY-SECRET-API-KEY" up

### Self-Hosted

./migrate -path ./database/dgraph/examples -database "https://some-url:9080?graphql=true&" up

# GraphQL 

If you want to access Migration info from GraphQL add the following to your schema:
```
type Migration {
    version: Int
    dirty: Boolean
}
```