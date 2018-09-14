package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v3"
	"github.com/golang-migrate/migrate/v3/database"
	"github.com/golang-migrate/migrate/v3/source"
)

const defaultTimeFormat = "20060102150405"

// set main log
var log = &Log{}

func init() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr,
			`Usage: migrate OPTIONS COMMAND [arg...]
       migrate [ --version | --help ]

Options:
  --config.source        directory of the configuration file (default "/cli/config")
  --config.file          configuration file name (without extension)
  --database.driver      database driver (default postgres)
  --database.address     address of the database (default "0.0.0.0:5432")
  --database.name        name of the database
  --database.user        database username (default "postgres")
  --database.password    database password (default "postgres")
  --database.ssl         database ssl mode (default "disable")
  --path                 Shorthand for -source=file://path
  --source               Location of the migrations (driver://url)
  --lock-timeout         Allow N seconds to acquire database lock (default 15)
  --prefetch             Number of migrations to load in advance before executing (default 10)
  --verbose              Print verbose logging (default true)
  --version              Print version
  --help                 Print usage

Commands:
  create [-ext E] [-dir D] [-seq] [-digits N] [-format] NAME
			   Create a set of timestamped up/down migrations titled NAME, in directory D with extension E.
			   Use -seq option to generate sequential up/down migrations with N digits.
			   Use -format option to specify a Go time format string.
  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop         Drop everyting inside database
  force V      Set version V but don't run migration (ignores dirty state)
  version      Print current migration version

Source drivers: `+strings.Join(source.List(), ", ")+`
Database drivers: `+strings.Join(database.List(), ", ")+"\n")
	}

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AddConfigPath(viper.GetString("config.source"))
	if viper.GetString("config.file") != "" {
		viper.SetConfigName(viper.GetString("config.file"))
		if err := viper.ReadInConfig(); err != nil {
			log.fatalf("cannot load configuration: %v", err)
		}
	}
}

func main() {
	help := viper.GetBool("help")
	version := viper.GetBool("version")
	verbose := viper.GetBool("verbose")
	prefetch := viper.GetInt("prefetch")
	lockTimeout := viper.GetInt("lock-timeout")
	path := viper.GetString("path")
	sourcePtr := viper.GetString("source")
	dbSource := dbMakeConnectionString(
		viper.GetString("database.driver"), viper.GetString("database.user"),
		viper.GetString("database.password"), viper.GetString("database.address"),
		viper.GetString("database.name"), viper.GetString("database.ssl"),
	)

	// initialize logger
	log.verbose = verbose

	// show cli version
	if version {
		fmt.Fprintln(os.Stderr, Version)
		os.Exit(0)
	}

	// show help
	if help {
		pflag.Usage()
		os.Exit(0)
	}

	// translate -path into -source if given
	if sourcePtr == "" && path != "" {
		sourcePtr = fmt.Sprintf("file://%v", path)
	}

	// initialize migrate
	// don't catch migraterErr here and let each command decide
	// how it wants to handle the error
	migrater, migraterErr := migrate.New(sourcePtr, dbSource)
	defer func() {
		if migraterErr == nil {
			migrater.Close()
		}
	}()
	if migraterErr == nil {
		migrater.Log = log
		migrater.PrefetchMigrations = uint(prefetch)
		migrater.LockTimeout = time.Duration(int64(lockTimeout)) * time.Second

		// handle Ctrl+c
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT)
		go func() {
			for range signals {
				log.Println("Stopping after this running migration ...")
				migrater.GracefulStop <- true
				return
			}
		}()
	}

	startTime := time.Now()

	switch pflag.Arg(0) {
	case "create":
		args := pflag.Args()[1:]
		seq := false
		seqDigits := 6

		createFlagSet := pflag.NewFlagSet("create", pflag.ExitOnError)
		extPtr := createFlagSet.String("ext", "", "File extension")
		dirPtr := createFlagSet.String("dir", "", "Directory to place file in (default: current working directory)")
		formatPtr := createFlagSet.String("format", defaultTimeFormat, `The Go time format string to use. If the string "unix" or "unixNano" is specified, then the seconds or nanoseconds since January 1, 1970 UTC respectively will be used. Caution, due to the behavior of time.Time.Format(), invalid format strings will not error`)
		createFlagSet.BoolVar(&seq, "seq", seq, "Use sequential numbers instead of timestamps (default: false)")
		createFlagSet.IntVar(&seqDigits, "digits", seqDigits, "The number of digits to use in sequences (default: 6)")
		createFlagSet.Parse(args)

		if createFlagSet.NArg() == 0 {
			log.fatal("error: please specify name")
		}
		name := createFlagSet.Arg(0)

		if *extPtr == "" {
			log.fatal("error: -ext flag must be specified")
		}
		*extPtr = "." + strings.TrimPrefix(*extPtr, ".")

		if *dirPtr != "" {
			*dirPtr = strings.Trim(*dirPtr, "/") + "/"
		}

		createCmd(*dirPtr, startTime, *formatPtr, name, *extPtr, seq, seqDigits)

	case "goto":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		if pflag.Arg(1) == "" {
			log.fatal("error: please specify version argument V")
		}

		v, err := strconv.ParseUint(pflag.Arg(1), 10, 64)
		if err != nil {
			log.fatal("error: can't read version argument V")
		}

		gotoCmd(migrater, uint(v))

		if log.verbose {
			log.Println("Finished after", time.Now().Sub(startTime))
		}

	case "up":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		limit := -1
		if pflag.Arg(1) != "" {
			n, err := strconv.ParseUint(pflag.Arg(1), 10, 64)
			if err != nil {
				log.fatal("error: can't read limit argument N")
			}
			limit = int(n)
		}

		upCmd(migrater, limit)

		if log.verbose {
			log.Println("Finished after", time.Now().Sub(startTime))
		}

	case "down":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		limit := -1
		if pflag.Arg(1) != "" {
			n, err := strconv.ParseUint(pflag.Arg(1), 10, 64)
			if err != nil {
				log.fatal("error: can't read limit argument N")
			}
			limit = int(n)
		}

		downCmd(migrater, limit)

		if log.verbose {
			log.Println("Finished after", time.Now().Sub(startTime))
		}

	case "drop":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		dropCmd(migrater)

		if log.verbose {
			log.Println("Finished after", time.Now().Sub(startTime))
		}

	case "force":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		if pflag.Arg(1) == "" {
			log.fatal("error: please specify version argument V")
		}

		v, err := strconv.ParseInt(pflag.Arg(1), 10, 64)
		if err != nil {
			log.fatal("error: can't read version argument V")
		}

		if v < -1 {
			log.fatal("error: argument V must be >= -1")
		}

		forceCmd(migrater, int(v))

		if log.verbose {
			log.Println("Finished after", time.Now().Sub(startTime))
		}

	case "version":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		versionCmd(migrater)

	default:
		pflag.Usage()
		os.Exit(0)
	}
}

func dbMakeConnectionString(driver, user, password, address, name, ssl string) string {
	return fmt.Sprintf("%s://%s:%s@%s/%s?sslmode=%s",
		driver, user, password, address, name, ssl,
	)
}
