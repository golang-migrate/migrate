# Big Query

See the official [Google Big Query Documentation](https://cloud.google.com/bigquery/docs) for more details.

The provided database URL must be in the following format:

`bigquery://{projectId}/{datasetId}?param=true`

where

- `projectId` is to be replaced with the name of the Google Cloud Platform (GCP) project Id where the Big Query instance has been created.
- `datasetId` is to be replaced with the name of the dataset where migrations will be executed.

| Param                    | WithInstance Config | Description                                                                                                                   |
| ------------------------ | ------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `x-migrations-table`     | `MigrationsTable`   | Name of the migrations table                                                                                                  |
| `x-stmt-timeout`         | `StmtTimeout`       | Duration after which queries are automatically terminated                                                                     |
| `x-gcp-credentials-file` |                     | Location of a credential JSON file to use for authenticating the Big Query client                                             |
| `x-insecure`             |                     | When true it specifies that no authentication should be used. To be used for testing purposes only                            |
| `x-endpoint`             |                     | Overrides the default endpoint to be used by the client when connecting to the database. To be used for testing purposes only |
