# OpenSearch

* Driver work with OpenSearch through [OpenSearch REST API](https://opensearch.org/docs/latest/getting-started/communicate/#opensearch-rest-api)
* Migrations are written in JSON format and support actions such as creating indices, updating mappings, modifying settings and etc.
* [Examples](./examples)

# Usage

`opensearch://user:password@host:port/index` 

| URL Query  | Default value | Description |
|------------|---------------------|-------------|
| `index` | `.migrations` | Name of the migrations index |
| `timeout` | `60s` | The max time that an operation will wait before failing. |
| `user` | | The user to sign in as. Can be omitted |
| `password` | | The user's password. Can be omitted | 
| `host` | | The host to connect to |
| `port` | | The port to bind to |