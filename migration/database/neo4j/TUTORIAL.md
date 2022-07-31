## Create migrations
Let's create nodes called `Users`:
```
migrate create -ext cypher -dir db/migrations -seq create_user_nodes
```
If there were no errors, we should have two files available under `db/migrations` folder:
- 000001_create_user_nodes.down.cypher
- 000001_create_user_nodes.up.cypher

Note the `cypher` extension that we provided.

In the `.up.cypher` file let's create the table:
```
CREATE (u1:User {name: "Peter"})
CREATE (u2:User {name: "Paul"})
CREATE (u3:User {name: "Mary"})
```
And in the `.down.sql` let's delete it:
```
MATCH (u:User) WHERE u.name IN ["Peter", "Paul", "Mary"] DELETE u
```
Ideally your migrations should be idempotent. You can read more about idempotency in [getting started](GETTING_STARTED.md#create-migrations)

## Run migrations
```
migrate -database ${NEO4J_URL} -path db/migrations up
```
Let's check if the table was created properly by running `bin/cypher-shell -u neo4j -p password`, then `neo4j> MATCH (u:User)`
The output you are supposed to see:
```
+-----------------------------------------------------------------+
| u                                                               |
+-----------------------------------------------------------------+
| (:User {name: "Peter")                                          |
| (:User {name: "Paul")                                           |
| (:User {name: "Mary")                                           |
+-----------------------------------------------------------------+
```
Great! Now let's check if running reverse migration also works:
```
migrate -database ${NEO4J_URL} -path db/migrations down
```
Make sure to check if your database changed as expected in this case as well.

## Database transactions

To show database transactions usage, let's create another set of migrations by running:
```
migrate create -ext cypher -dir db/migrations -seq add_mood_to_users
```
Again, it should create for us two migrations files:
- 000002_add_mood_to_users.down.cypher
- 000002_add_mood_to_users.up.cypher

In Neo4j, when we want our queries to be done in a transaction, we need to wrap it with `:BEGIN` and `:COMMIT` commands.
Migration up:
```
:BEGIN

MATCH (u:User)
SET u.mood = "Cheery"

:COMMIT
```
Migration down:
```
:BEGIN

MATCH (u:User)
SET u.mood = null

:COMMIT
```

## Optional: Run migrations within your Go app
Here is a very simple app running migrations for the above configuration:
```
import (
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/neo4j"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	m, err := migrate.New(
		"file://db/migrations",
		"neo4j://neo4j:password@localhost:7687/")
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil {
		log.Fatal(err)
	}
}
```