# Cassandra Driver

## Usage

```bash
migrate -url cassandra://host:port/keyspace -path ./db/migrations create add_field_to_table
migrate -url cassandra://host:port/keyspace -path ./db/migrations up
migrate help # for more info
```

Url format
- Authentication: `cassandra://username:password@host:port/keyspace`
- Cassandra v3.x: `cassandra://host:port/keyspace?protocol=4`


## Authors

* Paul Bergeron, https://github.com/dinedal
* Johnny Bergström, https://github.com/balboah
* pateld982, http://github.com/pateld982
