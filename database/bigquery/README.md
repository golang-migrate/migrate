# BigQuery

* Driver works with [Google Cloud BigQuery](https://cloud.google.com/bigquery/docs)
* [Examples](./examples)

### Usage
`bigquery://https://www.googleapis.com/bigquery/v2/?dataset_id=mydataset`

| Key                    | WithInstance Config | Default                  | Description                                                                                      |
|------------------------|---------------------|--------------------------|--------------------------------------------------------------------------------------------------|
| `x-migrations-table`   | `MigrationsTable`   | schema_migrations        | Name of the migrations table                                                                     |
| `x-statement-timeout`  | `StatementTimeout`  | 0                        | Abort any statement that takes more than the specified number of milliseconds                    |
| `credentials_filename` | -                   | -                        | The location of a credential JSON file.                                                          |
| `project_id`           | -                   | -                        | The current Google Cloud project ID.                                                             |
| `dataset_id`           | `DatasetID`         | -                        | ID of the default dataset in the current project.                                                |

### Environment variables:
- https://cloud.google.com/docs/authentication/application-default-credentials#GAC

| Key                            | Description                                      |
|--------------------------------|--------------------------------------------------|
| GOOGLE_APPLICATION_CREDENTIALS | The location of a credential JSON file.          | 

### Data definition language (DDL) statements in Google Standard SQL
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language

### Work with multi-statement queries
https://cloud.google.com/bigquery/docs/multi-statement-queries

