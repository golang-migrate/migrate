# PostgreSQL tutorial for beginners

## Create/configure database

For the purpose of this tutorial let's create PostgreSQL database called `example`.
Our user here is `postgres`, password `password`, and host is `localhost`.
```
psql -h localhost -U postgres -w -c "create database example;"
```
When using Migrate CLI we need to pass to database URL. Let's export it to a variable for convienience:
```
export POSTGRESQL_URL='postgres://postgres:password@localhost:5432/example?sslmode=disable'
```
`sslmode=disable` means that the connection with our database will not be encrypted. Enabling it is left as an exercise.

You can find further description of database URLs [here](README.md#database-urls).

## Create migrations
Let's create table called `users`:
```
migrate create -ext sql -dir db/migrations -seq create_users_table
```
If there were no errors, we should have two files available under `db/migrations` folder:
- 000001_create_users_table.down.sql
- 000001_create_users_table.up.sql

Note the `sql` extension that we provided.

In the `.up.sql` file let's create the table:
```
CREATE TABLE IF NOT EXISTS users(
   user_id serial PRIMARY KEY,
   username VARCHAR (50) UNIQUE NOT NULL,
   password VARCHAR (50) NOT NULL,
   email VARCHAR (300) UNIQUE NOT NULL
);
```
And in the `.down.sql` let's delete it:
```
DROP TABLE IF EXISTS users;
```
By adding `IF EXISTS/IF NOT EXISTS` we are making migrations idempotent - you can read more about idempotency in [getting started](../../GETTING_STARTED.md#create-migrations)

## Run migrations
```
migrate -database ${POSTGRESQL_URL} -path db/migrations up
```
Let's check if the table was created properly by running `psql example -c "\d users"`.
The output you are supposed to see:
```
                                    Table "public.users"
  Column  |          Type          |                        Modifiers                        
----------+------------------------+---------------------------------------------------------
 user_id  | integer                | not null default nextval('users_user_id_seq'::regclass)
 username | character varying(50)  | not null
 password | character varying(50)  | not null
 email    | character varying(300) | not null
Indexes:
    "users_pkey" PRIMARY KEY, btree (user_id)
    "users_email_key" UNIQUE CONSTRAINT, btree (email)
    "users_username_key" UNIQUE CONSTRAINT, btree (username)
```
Great! Now let's check if running reverse migration also works:
```
migrate -database ${POSTGRESQL_URL} -path db/migrations down
```
Make sure to check if your database changed as expected in this case as well.

## Database transactions

To show database transactions usage, let's create another set of migrations by running:
```
migrate create -ext sql -dir db/migrations -seq add_mood_to_users
```
Again, it should create for us two migrations files:
- 000002_add_mood_to_users.down.sql
- 000002_add_mood_to_users.up.sql

In Postgres, when we want our queries to be done in a transaction, we need to wrap it with `BEGIN` and `COMMIT` commands.
In our example, we are going to add a column to our database that can only accept enumerable values or NULL.
Migration up:
```
BEGIN;

CREATE TYPE enum_mood AS ENUM (
	'happy',
	'sad',
	'neutral'
);
ALTER TABLE users ADD COLUMN mood enum_mood;

COMMIT;
```
Migration down:
```
BEGIN;

ALTER TABLE users DROP COLUMN mood;
DROP TYPE enum_mood;

COMMIT;
```

Now we can run our new migration and check the database:
```
migrate -database ${POSTGRESQL_URL} -path db/migrations up
psql example -c "\d users"
```
Expected output:
```
                                    Table "public.users"
  Column  |          Type          |                        Modifiers                        
----------+------------------------+---------------------------------------------------------
 user_id  | integer                | not null default nextval('users_user_id_seq'::regclass)
 username | character varying(50)  | not null
 password | character varying(50)  | not null
 email    | character varying(300) | not null
 mood     | enum_mood              | 
Indexes:
    "users_pkey" PRIMARY KEY, btree (user_id)
    "users_email_key" UNIQUE CONSTRAINT, btree (email)
    "users_username_key" UNIQUE CONSTRAINT, btree (username)
```

## Optional: Run migrations within your Go app
Here is a very simple app running migrations for the above configuration:
```
import (
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	m, err := migrate.New(
		"file://db/migrations",
		"postgres://postgres:postgres@localhost:5432/example?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil {
		log.Fatal(err)
	}
}
```
You can find details [here](README.md#use-in-your-go-project)

## Fix issue where migrations run twice

When the schema and role names are the same, you might run into issues if you create this schema using migrations.
This is caused by the fact that the [default `search_path`](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH) is `"$user", public`.
In the first run (with an empty database) the migrate table is created in `public`.
When the migrations create the `$user` schema, the next run will store (a new) migrate table in this schema (due to order of schemas in `search_path`) and tries to apply all migrations again (most likely failing).

To solve this you need to change the default `search_path` by removing the `$user` component, so the migrate table is always stored in the (available) `public` schema.
This can only be done when using migrate from your own code, by creating the `driver` manually, so it can be used to configure the `search_path` before applying the migrations:
```golang
	db, err := sql.Open("postgres", dbURI)
	if err != nil {
		log.Fatalf("Unable to connect to the database: %s", err)
	}
	defer db.Close()

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatalf("Unable to initialize the driver: %s", err)
	}

	// Set search_path (default schema) to prevent issues where MigrationsTable is not stored in public schema
	// after a migration created a new schema, resulting in migrations getting executed more than once.
	if err := driver.Run(strings.NewReader("SET search_path TO public;")); err != nil {
		log.Fatalf("Failed to set search_path: %s", err)
	}

	m, err := migrate.NewWithDatabaseInstance("file://path/to/migrations", "postgres", driver)
	if err != nil {
		log.Fatalf("Cannot create the migrator: %s", err)
	}
```
