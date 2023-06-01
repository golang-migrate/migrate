# Google Cloud Spanner

## Usage

See [Google Spanner Documentation](https://cloud.google.com/spanner/docs) for
more details.

The DSN must be given in the following format.

`spanner://projects/{projectId}/instances/{instanceId}/databases/{databaseName}?param=true`

as described in [README.md#database-urls](../../README.md#database-urls)

| Param | WithInstance Config | Description |
| ----- | ------------------- | ----------- |
| `x-migrations-table` | `MigrationsTable` | Name of the migrations table |
| `x-clean-statements` | `CleanStatements` | Whether to parse and clean DDL statements before running migration towards Spanner (Required for comments and multiple statements) |
| `url` | `DatabaseName` | The full path to the Spanner database resource. If provided as part of `Config` it must not contain a scheme or query string to match the format `projects/{projectId}/instances/{instanceId}/databases/{databaseName}`|
| `projectId` || The Google Cloud Platform project id
| `instanceId` || The id of the instance running Spanner
| `databaseName` || The name of the Spanner database

> **Note:** Google Cloud Spanner migrations can take a considerable amount of 
> time. The migrations provided as part of the example take about 6 minutes to 
> run on a small instance.
>
> ```log
> 1481574547/u create_users_table (21.354507597s)
> 1496539702/u add_city_to_users (41.647359754s)
> 1496601752/u add_index_on_user_emails (2m12.155787369s)
> 1496602638/u create_books_table (2m30.77299181s)

## DDL with comments

At the moment the GCP Spanner backed does not seem to allow for comments (See https://issuetracker.google.com/issues/159730604)
so in order to be able to use migration with DDL containing comments `x-clean-statements` is required

## Multiple statements

In order to be able to use more than 1 DDL statement in the same migration file, the file has to be parsed and therefore the `x-clean-statements` flag is required

## Testing

To unit test the `spanner` driver, `SPANNER_DATABASE` needs to be set. You'll
need to sign-up to Google Cloud Platform (GCP) and have a running Spanner
instance since it is not possible to run Google Spanner outside GCP.
