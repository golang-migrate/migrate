# migrate CLI

## Installation

#### With Go toolchain

```
$ go get -u -d github.com/mattes/migrate/cli
$ go build -tags 'postgres' -o /usr/local/bin/migrate github.com/mattes/migrate/cli
```

#### MacOS

([todo #156](https://github.com/mattes/migrate/issues/156))

```
$ brew install migrate --with-postgres
```

#### Linux (*.deb package)

```
$ curl -L https://packagecloud.io/mattes/migrate/gpgkey | apt-key add -
$ echo "deb https://packagecloud.io/mattes/migrate/ubuntu/ xenial main" > /etc/apt/sources.list.d/migrate.list
$ apt-get update
$ apt-get install -y migrate
```

#### Download pre-build binary (Windows, MacOS, or Linux)

[Release Downloads](https://github.com/mattes/migrate/releases)

```
$ curl -L https://github.com/mattes/migrate/releases/download/$version/migrate.$platform-amd64.tar.gz | tar xvz
```



## Usage

```
$ migrate -help
Usage: migrate OPTIONS COMMAND [arg...]
       migrate [ -version | -help ]

Options:
  -source          Location of the migrations (driver://url)
  -path            Shorthand for -source=file://path 
  -database        Run migrations against this database (driver://url)
  -prefetch N      Number of migrations to load in advance before executing (default 10)
  -lock-timeout N  Allow N seconds to acquire database lock (default 15)
  -verbose         Print verbose logging
  -version         Print version
  -help            Print usage

Commands:
  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop         Drop everyting inside database
  force V      Set version V but don't run migration (ignores dirty state)
  version      Print current migration version
```


So let's say you want to run the first two migrations

```
$ migrate -database postgres://localhost:5432/database up 2
```

If your migrations are hosted on github

```
$ migrate -source github://mattes:personal-access-token@mattes/migrate_test \
    -database postgres://localhost:5432/database down 2
```

The CLI will gracefully stop at a safe point when SIGINT (ctrl+c) is received.
Send SIGKILL for immediate halt.



## Reading CLI arguments from somewhere else

##### ENV variables

```
$ migrate -database "$MY_MIGRATE_DATABASE"
```

##### JSON files

Check out https://stedolan.github.io/jq/

```
$ migrate -database "$(cat config.json | jq '.database')"
```

##### YAML files

````
$ migrate -database "$(cat config/database.yml | ruby -ryaml -e "print YAML.load(STDIN.read)['database']")"
$ migrate -database "$(cat config/database.yml | python -c 'import yaml,sys;print yaml.safe_load(sys.stdin)["database"]')"
```
