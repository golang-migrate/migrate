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
| `x-clean-statements` | `CleanStatements` | **Deprecated.** This parameter is ignored. Statements are now always automatically parsed and comments are stripped. |
| `url` | `DatabaseName` | The full path to the Spanner database resource. If provided as part of `Config` it must not contain a scheme or query string to match the format `projects/{projectId}/instances/{instanceId}/databases/{databaseName}`|
| `projectId` || The Google Cloud Platform project id |
| `instanceId` || The id of the instance running Spanner |
| `databaseName` || The name of the Spanner database |

> **Note:** Google Cloud Spanner DDL migrations can take a considerable amount of
> time. The migrations provided as part of the example take about 6 minutes to
> run on a small instance.
>
> ```log
> 1481574547/u create_users_table (21.354507597s)
> 1496539702/u add_city_to_users (41.647359754s)
> 1496601752/u add_index_on_user_emails (2m12.155787369s)
> 1496602638/u create_books_table (2m30.77299181s)
> ```

## Supported Statement Types

The Spanner driver supports three types of statements in migration files:

### DDL (Data Definition Language)

Schema modification statements like `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, etc.
These are executed using Spanner's `UpdateDatabaseDdl` API.

```sql
CREATE TABLE Users (
    UserId INT64 NOT NULL,
    Name STRING(100)
) PRIMARY KEY(UserId);
```

### DML (Data Manipulation Language) - INSERT

`INSERT` statements are executed within a read-write transaction, allowing multiple inserts to be atomic.

```sql
INSERT INTO Users (UserId, Name) VALUES (1, 'Alice');
INSERT INTO Users (UserId, Name) VALUES (2, 'Bob');
```

### Partitioned DML - UPDATE and DELETE

`UPDATE` and `DELETE` statements are executed using Spanner's `PartitionedUpdate` API,
which is optimized for large-scale data modifications.

```sql
UPDATE Users SET Name = 'Updated' WHERE UserId = 1;
```

```sql
DELETE FROM Users WHERE UserId = 1;
```

### Statement Type Restrictions

**Important:** Each migration file must contain only one type of statement. You cannot mix:

- DDL with DML
- INSERT with UPDATE/DELETE

For example, the following migration file will fail:

```sql
-- This will fail: mixing INSERT and UPDATE
INSERT INTO Users (UserId, Name) VALUES (1, 'Alice');
UPDATE Users SET Name = 'Bob' WHERE UserId = 1;
```

## Comments in Migrations

Migration files can contain SQL comments. The driver automatically parses and strips comments
before execution since Spanner's `UpdateDatabaseDdl` API does not support comments.

Supported comment styles:

- Single-line comments: `-- comment`
- Multi-line comments: `/* comment */`

```sql
-- This migration creates the users table
/*
 * Author: migrate
 * Description: Initial schema setup
 */
CREATE TABLE Users (
    UserId INT64 NOT NULL, -- primary key
    Name STRING(100)
) PRIMARY KEY(UserId);
```

## Multiple Statements

Multiple statements of the same type can be included in a single migration file,
separated by semicolons:

```sql
CREATE TABLE Users (
    UserId INT64 NOT NULL
) PRIMARY KEY(UserId);

CREATE INDEX UsersByName ON Users(Name);
```

## Testing

The Spanner driver can be tested using the Spanner emulator provided by the
`cloud.google.com/go/spanner/spannertest` package. The unit tests use this
emulator and do not require a real Spanner instance.

For integration testing against a real Spanner instance, set the `SPANNER_DATABASE`
environment variable to your database's full resource path.
