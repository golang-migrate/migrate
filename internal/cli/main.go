package cli

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source"
)

const defaultTimeFormat = "20060102150405"

// set main log
var log = &Log{}

// Main function of a cli application. It is public for backwards compatibility with `cli` package
func Main(version string) {
	helpPtr := flag.Bool("help", false, "")
	versionPtr := flag.Bool("version", false, "")
	verbosePtr := flag.Bool("verbose", false, "")
	prefetchPtr := flag.Uint("prefetch", 10, "")
	lockTimeoutPtr := flag.Uint("lock-timeout", 15, "")
	pathPtr := flag.String("path", "", "")
	databasePtr := flag.String("database", "", "")
	sourcePtr := flag.String("source", "", "")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr,
			`Usage: migrate OPTIONS COMMAND [arg...]
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
			   Use -format option to specify a Go time format string. Note: migrations with the same time cause "duplicate migration version" error. 
  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop         Drop everything inside database
  force V      Set version V but don't run migration (ignores dirty state)
  version      Print current migration version

Source drivers: `+strings.Join(source.List(), ", ")+`
Database drivers: `+strings.Join(database.List(), ", ")+"\n")
	}

	flag.Parse()

	// initialize logger
	log.verbose = *verbosePtr

	// show cli version
	if *versionPtr {
		fmt.Fprintln(os.Stderr, version)
		os.Exit(0)
	}

	// show help
	if *helpPtr {
		flag.Usage()
		os.Exit(0)
	}

	// translate -path into -source if given
	if *sourcePtr == "" && *pathPtr != "" {
		*sourcePtr = fmt.Sprintf("file://%v", *pathPtr)
	}

	// initialize migrate
	// don't catch migraterErr here and let each command decide
	// how it wants to handle the error
	migrater, migraterErr := migrate.New(*sourcePtr, *databasePtr)
	defer func() {
		if migraterErr == nil {
			if _, err := migrater.Close(); err != nil {
				log.Println(err)
			}
		}
	}()
	if migraterErr == nil {
		migrater.Log = log
		migrater.PrefetchMigrations = *prefetchPtr
		migrater.LockTimeout = time.Duration(int64(*lockTimeoutPtr)) * time.Second

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

	switch flag.Arg(0) {
	case "create":
		args := flag.Args()[1:]
		seq := false
		seqDigits := 6

		createFlagSet := flag.NewFlagSet("create", flag.ExitOnError)
		extPtr := createFlagSet.String("ext", "", "File extension")
		dirPtr := createFlagSet.String("dir", "", "Directory to place file in (default: current working directory)")
		formatPtr := createFlagSet.String("format", defaultTimeFormat, `The Go time format string to use. If the string "unix" or "unixNano" is specified, then the seconds or nanoseconds since January 1, 1970 UTC respectively will be used. Caution, due to the behavior of time.Time.Format(), invalid format strings will not error`)
		createFlagSet.BoolVar(&seq, "seq", seq, "Use sequential numbers instead of timestamps (default: false)")
		createFlagSet.IntVar(&seqDigits, "digits", seqDigits, "The number of digits to use in sequences (default: 6)")
		if err := createFlagSet.Parse(args); err != nil {
			log.Println(err)
		}

		if createFlagSet.NArg() == 0 {
			log.fatal("error: please specify name")
		}
		name := createFlagSet.Arg(0)

		if *extPtr == "" {
			log.fatal("error: -ext flag must be specified")
		}

		if err := createCmd(*dirPtr, startTime, *formatPtr, name, *extPtr, seq, seqDigits, true); err != nil {
			log.fatalErr(err)
		}

	case "goto":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		if flag.Arg(1) == "" {
			log.fatal("error: please specify version argument V")
		}

		v, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err != nil {
			log.fatal("error: can't read version argument V")
		}

		if err := gotoCmd(migrater, uint(v)); err != nil {
			log.fatalErr(err)
		}

		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}

	case "up":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		limit := -1
		if flag.Arg(1) != "" {
			n, err := strconv.ParseUint(flag.Arg(1), 10, 64)
			if err != nil {
				log.fatal("error: can't read limit argument N")
			}
			limit = int(n)
		}

		if err := upCmd(migrater, limit); err != nil {
			log.fatalErr(err)
		}

		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}

	case "down":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		downFlagSet := flag.NewFlagSet("down", flag.ExitOnError)
		applyAll := downFlagSet.Bool("all", false, "Apply all down migrations")

		args := flag.Args()[1:]
		if err := downFlagSet.Parse(args); err != nil {
			log.fatalErr(err)
		}

		downArgs := downFlagSet.Args()
		num, needsConfirm, err := numDownMigrationsFromArgs(*applyAll, downArgs)
		if err != nil {
			log.fatalErr(err)
		}
		if needsConfirm {
			log.Println("Are you sure you want to apply all down migrations? [y/N]")
			var response string
			fmt.Scanln(&response)
			response = strings.ToLower(strings.TrimSpace(response))

			if response == "y" {
				log.Println("Applying all down migrations")
			} else {
				log.fatal("Not applying all down migrations")
			}
		}

		if err := downCmd(migrater, num); err != nil {
			log.fatalErr(err)
		}

		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}

	case "drop":
		log.Println("Are you sure you want to drop the entire database schema? [y/N]")
		var response string
		fmt.Scanln(&response)
		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" {
			log.Println("Dropping the entire database schema")
		} else {
			log.fatal("Aborted dropping the entire database schema")
		}

		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		if err := dropCmd(migrater); err != nil {
			log.fatalErr(err)
		}

		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}

	case "force":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		if flag.Arg(1) == "" {
			log.fatal("error: please specify version argument V")
		}

		v, err := strconv.ParseInt(flag.Arg(1), 10, 64)
		if err != nil {
			log.fatal("error: can't read version argument V")
		}

		if v < -1 {
			log.fatal("error: argument V must be >= -1")
		}

		if err := forceCmd(migrater, int(v)); err != nil {
			log.fatalErr(err)
		}

		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}

	case "version":
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}

		if err := versionCmd(migrater); err != nil {
			log.fatalErr(err)
		}

	default:
		flag.Usage()

		// If a command is not found we exit with a status 2 to match the behavior
		// of flag.Parse() with flag.ExitOnError when parsing an invalid flag.
		os.Exit(2)
	}
}
