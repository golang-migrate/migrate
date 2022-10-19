# Google Cloud BigQuery

## Usage

See [Google BigQuery Documentation](https://cloud.google.com/bigquery/docs) for
more details.

The DSN must be given in the following format.

`bigquery://projects/{projectId}/datasets/{datasetName}?param=true`

as described in [README.md#database-urls](../../README.md#database-urls)

| Param | WithInstance Config | Description |
| ----- | ------------------- | ----------- |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `url` | `DatabaseName` | The full path to the BigQuery dataset. If provided as part of `Config` it must not contain a scheme or query string to match the format `projects/{projectId}/locations/{location}/dataset/{datasetName}`|
| `projectId` || The Google Cloud Platform project id
| `location` || The location of the BigQuery dataset
| `datasetName` || The name of the BigQuery dataset

> **Note:** Log of the migrations provided as part of the example:
>
> ```log
> 1481574547/u create_users_table (1.623219875s)
> 1496539702/u add_city_to_users (3.336193375s)
> 1496602638/u create_books_table (4.918687583s)
> 1621360367/u create_transactions_table (6.512974834s)

## Testing

To unit test the `bigquery` driver, `GCLOUD_PROJECT_ID` needs to be set. You'll
need to sign-up to Google Cloud Platform (GCP), have a project, the BigQuery API 
enabled and create a BigQuery dataset, since it is not possible to run Google 
BigQuery outside GCP.
