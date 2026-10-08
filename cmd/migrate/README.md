# migrate CLI

## Installation

### Download pre-built binary (Windows, MacOS, or Linux)

[Release Downloads](https://github.com/golang-migrate/migrate/releases)

```bash
$ curl -L https://github.com/golang-migrate/migrate/releases/download/$version/migrate.$os-$arch.tar.gz | tar xvz
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
$ curl -fsSL https://packagecloud.io/golang-migrate/migrate/gpgkey | sudo gpg --dearmor -o /etc/apt/keyrings/migrate.gpg
$ echo "deb [signed-by=/etc/apt/keyrings/migrate.gpg] https://packagecloud.io/golang-migrate/migrate/ubuntu/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/migrate.list
$ apt-get update
$ apt-get install -y migrate
```

### With Go toolchain

Requires Go 1.16+ (for `go install <package>@<version>`). The `-tags` flag
selects which database and source drivers are compiled in.

```bash
# a specific release
$ go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.19.1

# latest release
$ go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# multiple database and source drivers
$ go install -tags 'postgres mysql file github' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

The binary is installed to `$(go env GOBIN)`, or `$(go env GOPATH)/bin` if
`GOBIN` is unset.

#### Notes

1. `go install <package>@<version>` needs Go 1.16 or later. `go get` no longer
installs command binaries as of Go 1.18 — use `go install`.
1. These examples build the CLI with only the `postgres` database driver. To
build for other databases, replace the `postgres` build tag with the
appropriate tag(s). The tags correspond to the names of the sub-packages under
the [`database`](../../database) package.
1. Likewise, to support sources other than `file`, add the appropriate build
tag(s) from the [`source`](../../source) package.
1. Support for build constraints will be removed in the future: https://github.com/golang-migrate/migrate/issues/60

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
  create [-ext E] [-dir D] [-seq] [-digits N] [-format] [-tz] NAME
           Create a set of timestamped up/down migrations titled NAME, in directory D with extension E.
           Use -seq option to generate sequential up/down migrations with N digits.
           Use -format option to specify a Go time format string. Note: migrations with the same time cause "duplicate migration version" error.
           Use -tz option to specify the timezone that will be used when generating non-sequential migrations (defaults: UTC).

  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N] [-all]    Apply all or N down migrations
        Use -all to apply all down migrations
  drop [-f]    Drop everything inside database
        Use -f to bypass confirmation
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
$ migrate -database "$(cat config.json | jq -r '.database')"
```

### YAML files

```bash
$ migrate -database "$(cat config/database.yml | ruby -ryaml -e "print YAML.load(STDIN.read)['database']")"
$ migrate -database "$(cat config/database.yml | python -c 'import yaml,sys;print yaml.safe_load(sys.stdin)["database"]')"
```
