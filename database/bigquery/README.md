# BigQuery (Beta)

* Driver works with Google Cloud BigQuery
* [Examples](./examples)

### Usage
`bigquery://https://bigquery.googleapis.com/bigquery/v2/?x-migrations-table=schema_migrations&x-statement-timeout=0&credentials_filename=./myproject-XXXXXXXXXXXXX-XXXXXXXXXXXX.json&project_id=myproject-XXXXXXXXXXXXX&dataset_id=mydataset`


### System variables reference
https://cloud.google.com/bigquery/docs/reference/system-variables

| Key                      | WithInstance Config | Default                  | Description                                                                                                                                                                                                                                                                                                                                                                                    |
|--------------------------|---------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `x-migrations-table`     | `MigrationsTable`   | schema_migrations        | Name of the migrations table                                                                                                                                                                                                                                                                                                                                                                   |
| `x-statement-timeout`    | `StatementTimeout`  | 0                        | Abort any statement that takes more than the specified number of milliseconds                                                                                                                                                                                                                                                                                                                  |
| `credentials_filename`   | -                   | -                        | The location of a credential JSON file.                                                                                                                                                                                                                                                                                                                                                        |
| `project_id`             | -                   | -                        | The current Google Cloud project ID.                                                                                                                                                                                                                                                                                                                                                           |
| `dataset_id`             | `DatasetID`         | -                        | ID of the default dataset in the current project. This ID is used when a dataset is not specified for a project in the query. You can use the SET statement to assign @@dataset_id to another dataset ID in the current project. The system variables @@dataset_project_id and @@dataset_id can be set and used together.                                                                      |
| `dataset_project_id`     | `DatasetProjectID`  | the same as `project_id` | ID of the default project that's used when one is not specified for a dataset used in the query. If @@dataset_project_id is not set, or if it is set to NULL, the query-executing project (@@project_id) is used. You can use the SET statement to assign @@dataset_project_id to another project ID. The system variables @@dataset_project_id and @@dataset_id can be set and used together. |
| `query_label`            | `QueryLabel`        | NULL                     | Query label to associate with query jobs in the current multi-statement query or session. If set in a query, all subsequent query jobs in the script or session will have this label. If not set in a query, the value for this system variable is NULL. For an example of how to set this system variable, see Associate jobs in a session with a label.                                      |
| `time_zone`              | `TimeZone`          | UTC                      | The default time zone to use in time zone-dependent SQL functions, when a time zone is not specified as an argument. @@time_zone can be modified by using a SET statement to any valid time zone name. At the start of each script, @@time_zone begins as “UTC”.                                                                                                                               |



### Environment variables:
- https://cloud.google.com/docs/authentication/application-default-credentials#GAC

| Key                            | Description                                      |
|--------------------------------|--------------------------------------------------|
| GOOGLE_APPLICATION_CREDENTIALS | The location of a credential JSON file.          | 


### Data definition language (DDL) statements in Google Standard SQL
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language


### Work with multi-statement queries
https://cloud.google.com/bigquery/docs/multi-statement-queries

