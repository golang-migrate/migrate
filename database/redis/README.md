# redis

URL format:

- standalone connection: 

`redis://<user>:<password>@<host>:<port>/<db_number>`

- failover connection:

`redis://<user>:<password>@/<db_number>?sentinel_addr=<sentinel_host>:<sentinel_port>`

- cluster connection:

`redis://<user>:<password>@<host>:<port>?addr=<host2>:<port2>&addr=<host3>:<port3>`

`rediss://<user>:<password>@<host>:<port>?addr=<host2>:<port2>&addr=<host3>:<port3>`

| URL Query          | WithInstance Config | Description                                 |
|--------------------|---------------------|---------------------------------------------|
| `x-mode`           | -                   | The Mode that used to choose client type    |
| `x-migrations-key` | `MigrationsKey`     | Specify the key where migrations are stored |
| `x-lock-key`       | `LockKey`           | Specify the key where locks are stored      |
| `x-lock-timeout`   | `LockTimeout`       | Specify the timeout of lock                 |
