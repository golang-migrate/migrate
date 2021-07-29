# migrate CLI

## Installation

### Download pre-built binary (Windows, MacOS, or Linux)

[Release Downloads](https://github.com/golang-migrate/migrate/releases)

```bash
$ curl -L https://github.com/golang-migrate/migrate/releases/download/$version/migrate.$platform-amd64.tar.gz | tar xvz
```

### MacOS

```bash
$ brew install golang-migrate
```

### Windows

Using [scoop](https://scoop.sh/)

```bash
$ scoop install migrate
```

### Linux (*.deb package)

```bash
$ curl -L https://packagecloud.io/golang-migrate/migrate/gpgkey | apt-key add -
$ echo "deb https://packagecloud.io/golang-migrate/migrate/ubuntu/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/migrate.list
$ apt-get update
$ apt-get install -y migrate
```

### With Go toolchain

#### Versioned

```bash
$ go get -u -d github.com/golang-migrate/migrate/cmd/migrate
$ cd $GOPATH/src/github.com/golang-migrate/migrate/cmd/migrate
$ git checkout $TAG  # e.g. v4.1.0
$ # Go 1.15 and below
$ go build -tags 'postgres' -ldflags="-X main.Version=$(git describe --tags)" -o $GOPATH/bin/migrate $GOPATH/src/github.com/golang-migrate/migrate/cmd/migrate
$ # Go 1.16+
$ go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@$TAG
```

#### Unversioned

```bash
$ # Go 1.15 and below
$ go get -tags 'postgres' -u github.com/golang-migrate/migrate/cmd/migrate
$ # Go 1.16+
$ go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

#### Notes

1. Requires a version of Go that [supports modules](https://golang.org/cmd/go/#hdr-Preliminary_module_support). e.g. Go 1.11+
1. These examples build the cli which will only work with postgres.  In order
to build the cli for use with other databases, replace the `postgres` build tag
with the appropriate database tag(s) for the databases desired.  The tags
correspond to the names of the sub-packages underneath the
[`database`](../database) package.
1. Similarly to the database build tags, if you need to support other sources, use the appropriate build tag(s).
1. Support for build constraints will be removed in the future: https://github.com/golang-migrate/migrate/issues/60
1. For versions of Go 1.15 and lower, [make sure](https://github.com/golang-migrate/migrate/pull/257#issuecomment-705249902) you're not installing the `migrate` CLI from a module. e.g. there should not be any `go.mod` files in your current directory or any directory from your current directory to the root

## Usage

```bash
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
  create [-ext E] [-dir D] [-seq] [-digits N] [-format] NAME
               Create a set of timestamped up/down migrations titled NAME, in directory D with extension E.
               Use -seq option to generate sequential up/down migrations with N digits.
               Use -format option to specify a Go time format string.
  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop         Drop everything inside database
  force V      Set version V but don't run migration (ignores dirty state)
  version      Print current migration version
```

So let's say you want to run the first two migrations

```bash
$ migrate -source file://path/to/migrations -database postgres://localhost:5432/database up 2
```

If your migrations are hosted on github

```bash
$ migrate -source github://mattes:personal-access-token@mattes/migrate_test \
    -database postgres://localhost:5432/database down 2
```

The CLI will gracefully stop at a safe point when SIGINT (ctrl+c) is received.
Send SIGKILL for immediate halt.

## Reading CLI arguments from somewhere else

### ENV variables

```bash
$ migrate -database "$MY_MIGRATE_DATABASE"
```

### JSON files

Check out https://stedolan.github.io/jq/

```bash
$ migrate -database "$(cat config.json | jq '.database')"
```

### YAML files

```bash
$ migrate -database "$(cat config/database.yml | ruby -ryaml -e "print YAML.load(STDIN.read)['database']")"
$ migrate -database "$(cat config/database.yml | python -c 'import yaml,sys;print yaml.safe_load(sys.stdin)["database"]')"
```
