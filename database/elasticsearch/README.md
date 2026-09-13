# Elasticsearch

* Driver talks to the cluster over its [REST API](https://www.elastic.co/docs/api/doc/elasticsearch), so it works against any 7.x, 8.x or 9.x cluster, with no client library pinned to a major version.
* Migrations are plain HTTP requests. A file holds one or more requests, separated by a line containing only `---`, and they are sent in order.
* [Examples](./examples)

## Usage

`elasticsearch://user:password@host:port/?query` for plain HTTP, `elasticsearch+https://user:password@host:port/?query` for HTTPS.

The scheme only selects the transport; every alias below maps to the same driver, so pick whichever reads best:

| Transport | Schemes                                 |
| --------- | --------------------------------------- |
| HTTP      | `elasticsearch`, `elasticsearch+http`   |
| HTTPS     | `elasticsearchs`, `elasticsearch+https` |

The path of the URL, if any, is used as a prefix for every request, for clusters served behind a reverse proxy (`elasticsearch://proxy:8080/es/`).

| URL Query                          | WithInstance Config  | Description                                                                                                                                                                                                  |
| ---------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `x-migrations-index`               | `MigrationsIndex`    | Name of the index holding the version. Defaults to `schema_migrations`                                                                                                                                       |
| `x-drop-indices-pattern`           | `DropIndicesPattern` | Index and data stream pattern deleted by `Drop`. Defaults to `*`, i.e. **every non-system index and data stream of the cluster**                                                                             |
| `x-request-timeout`                |                      | The timeout in seconds of a single request, `0` to disable it. Defaults to `30`. A migration running a long operation such as `_reindex` needs this raised, or it fails while the cluster carries on working |
| `x-advisory-locking`               | `Locking.Enabled`    | Feature flag for advisory locking, if set to `false`, disable advisory locking. Defaults to `true`                                                                                                           |
| `x-advisory-lock-index`            | `Locking.IndexName`  | The name of the index to use for advisory locking. Defaults to `migrate_advisory_lock`                                                                                                                       |
| `x-advisory-lock-timeout`          | `Locking.Timeout`    | The max time in seconds that migrate will wait to acquire a lock before failing. Defaults to `15`                                                                                                            |
| `x-advisory-lock-timeout-interval` | `Locking.Interval`   | The max time in seconds between attempts to acquire the advisory lock, which is retried using an exponential backoff algorithm. Defaults to `10`                                                             |
| `user`                             |                      | The user to sign in as. Can be omitted                                                                                                                                                                       |
| `password`                         |                      | The user's password. Can be omitted                                                                                                                                                                          |
| `host`                             |                      | The host to connect to                                                                                                                                                                                       |
| `port`                             |                      | The port to bind to                                                                                                                                                                                          |

`WithInstance` takes an `*http.Client`, so TLS settings, custom headers and any other transport concern are configured there:

```go
client := &http.Client{Timeout: 30 * time.Second, Transport: myTransport}
clusterURL, _ := url.Parse("https://elastic:changeme@localhost:9200/")
driver, err := elasticsearch.WithInstance(client, clusterURL, &elasticsearch.Config{
  Locking: elasticsearch.Locking{Enabled: true},
})
```

Note that, unlike the URL form, `WithInstance` takes `Locking.Enabled` at face value: advisory locking is off unless it is set.

## Migration format

A migration is a list of HTTP requests separated by a line holding only `---`:

```http
PUT /books HTTP/1.1
Content-Type: application/json

{
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "author": { "type": "text" },
      "published_year": { "type": "integer" }
    }
  }
}

---
POST /books/_doc/1?refresh=true HTTP/1.1
Content-Type: application/json

{ "title": "The Hitchhiker's Guide to the Galaxy", "author": "Douglas Adams", "published_year": 1979 }
```

* The request line is `METHOD /path[?query]`, optionally followed by an HTTP version, which is ignored. The path is relative to the cluster URL and must start with `/`.
* Headers follow the request line, one per line, and end at the first blank line. Everything after that blank line is the body.
* The method must be a standard one: `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `CONNECT`, `OPTIONS`, `TRACE` or `QUERY`. Anything else is rejected rather than sent, so that a typo in the request line fails the migration instead of puzzling the cluster.
* `Content-Type` defaults to `application/json` when a request has a body.
* The body keeps its trailing newline, which the ndjson apis (`_bulk`, `_msearch`) require.
* A leading `---` is allowed, so a file may start with a delimiter.
* Lines starting with `#` or `//` before the request line are comments.
* A `---` inside a request body is left alone: only a line holding nothing else is a delimiter.

Any response outside the 2xx range fails the migration, and the error reports the line of the offending request.

## Caveats

* **Elasticsearch has no transactions.** If a request fails halfway through a file, the requests before it stay applied and the migration is marked dirty. Writing migrations so that each request is idempotent makes recovery easier.
* **`Drop` defaults to deleting every non-system index and data stream of the cluster**, which is what the driver interface asks for but is rarely what you want on a shared cluster. Set `x-drop-indices-pattern` to scope it. Indices are resolved to concrete names and deleted by name, so `Drop` also works on clusters with `action.destructive_requires_name` enabled, the default as of Elasticsearch 8. Data streams are deleted through the stream itself, since their backing indices cannot be removed one by one. Index templates and ILM policies are left alone.
* Writes to the version and lock documents use `refresh=true`, so the version is visible immediately rather than after the index refresh interval.
* Advisory locking creates a single document with `op_type=create` in the lock index. A process killed mid-migration leaves that document behind, and the lock must then be released by hand: `DELETE /migrate_advisory_lock/_doc/1`.
