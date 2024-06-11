# YDB

See [YDB Documentation](https://ydb.tech/docs/en/) for more details.

## Usage

The DSN must be given in the following format.

`ydb://{endpoint}/{database}[?param=value]`

| Param | WithInstance Config | Description |
| ----- | ------------------- | ----------- |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table. Defaults to `schema_migrations`. |
| `x-insecure` | | Boolean to indicate whether to use an insecure connection. Defaults to `false`. |
| `x-connect-timeout` | | Initial connection timeout to the cluster. Unset/empty means no timeout. |
| `x-auth-mode` | | Options: (unset/empty), `anonymous`, `static`, `access-token`, `oauth2`. |

`x-connect-timeout` is parsed using [time.ParseDuration](https://pkg.go.dev/time#ParseDuration)

### Auth

#### Mode `static`

| Param | Description |
| ----- | ----------- |
| `x-auth-username` | Username |
| `x-auth-password` | Password |

#### Mode `access-token`

| Param | Description |
| ----- | ----------- |
| `x-auth-access-token` | Access Token |

#### Mode `oauth2`

| Param | Description |
| ----- | ----------- |
| `x-auth-token-endpoint` | [Token Endpoint](https://www.rfc-editor.org/rfc/rfc6749#section-3.2) |
| `x-auth-grant-type` | [Grant Type](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.2) |
| `x-auth-resource` | [Resource](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.4) |
| `x-auth-audience` | [Audience](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.6) |
| `x-auth-scope` | [Scope](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.8) |
| `x-auth-requested-token-type` | [Requested Token Type](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.10) |
| `x-auth-subject-token-source` | Options: `fixed`, `jwt`. |

##### Subject Token Source `fixed`

| Param | Description |
| ----- | ----------- |
| `x-auth-subject-token` | [Token](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.12) |
| `x-auth-subject-token-type` | [Token Type](https://www.rfc-editor.org/rfc/rfc8693#section-2.1-4.14) |

##### Subject Token Source `jwt`

| Param | Description |
| ----- | ----------- |
| `x-auth-subject-jwt-iss` | [Issuer](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.1) |
| `x-auth-subject-jwt-sub` | [Subject](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.2) |
| `x-auth-subject-jwt-aud` | [Audience](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.3) |
| `x-auth-subject-jwt-jti` | [JWT ID](https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.7) |
| `x-auth-subject-jwt-alg` | [Algorithm](https://datatracker.ietf.org/doc/html/rfc7515#section-4.1.1) |
| `x-auth-subject-jwt-kid` | [Key ID](https://datatracker.ietf.org/doc/html/rfc7515#section-4.1.4) |
| `x-auth-subject-jwt-pem-file` | Private Key |
