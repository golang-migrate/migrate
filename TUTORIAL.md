# Getting started
Before you start, you should understand the concept of forward/up and reverse/down database migrations.

Configure a database for your application. Make sure that your database driver is supported [here](README.md#databases)
For the purpose of this tutorial let's create PostgreSQL database called `example`.
Our user here is `postgres`, password `password`, and host is `localhost`.
```
psql -h localhost -U postgres -w -c "create database example;"
```
When using Migrate CLI we need to pass to database URL. Let's export it to a variable for convienience:
```
export POSTGRESQL_URL=postgres://postgres:password@localhost:5432/example?sslmode=disable
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
**IMPORTANT:** In a project developed by more than one person there is a small probability of migrations incosistency - e.g. two developers can create conflicting migrations, and the developer that created his migration later gets it merged to the repository first.
Keep an eye on such cases (and be even more careful when cherry picking).

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
By adding `IF EXISTS/IF NOT EXISTS` we are making migrations idempotent - we can run the same sql code twice in a row with the same result. This makes our migrations more robust. On the other hand, it causes slightly less control over database schema - e.g. let's say you forgot to drop the table in down migration. You run down migration - the table is still there. When you run up migration again - `CREATE TABLE` would return an error, helping you find an issue in down migration, while `CREATE TABLE IF NOT EXISTS` would not. Use those conditions wisely.

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

**Before commiting your migrations:** You should run your migrations up, down, and then up again to see if migrations are working properly both ways.
(e.g. if you created a table in a migration but reverse migration did not delete it, you will encounter an error when running the forward migration again)
It's also worth checking your migrations in a separate, containerized environment. You can find some tools in the end of this tutorial.

**Hint:** Most probably you are going to use to the above commands often, it's worth putting them in a Makefile of your project.

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

Just add the code to your app and you're ready to go!

## Further reading:
- [Best practices](MIGRATIONS.md)
- [FAQ](FAQ.md)
- Tools for testing your migrations in a container:
	- https://github.com/dhui/dktest
	- https://github.com/ory/dockertest
