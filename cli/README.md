# CLI

## Installation

```
# dowload, build and install the CLI tool
# -tags takes database and source drivers and will only build those
$ go get -u -tags 'postgres' -o migrate github.com/mattes/migrate/cli
```


## Usage

```
$ migrate -help
Usage: migrate OPTIONS COMMAND [arg...]
       migrate [ -version | -help ]

Options:
  -source      Location of the migrations (driver://url)
  -path        Shorthand for -source=file://path
  -database    Run migrations against this database (driver://url)
  -prefetch N  Number of migrations to load in advance before executing (default 10)
  -verbose     Print verbose logging
  -version     Print version
  -help        Print usage

Commands:
  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop         Drop everyting inside database
  version      Print current migration version


# so let's say you want to run the first two migrations
migrate -database postgres://localhost:5432/database up 2

# if your migrations are hosted on github
migrate -source github://mattes:personal-access-token@mattes/migrate_test \
  -database postgres://localhost:5432/database down 2
```

