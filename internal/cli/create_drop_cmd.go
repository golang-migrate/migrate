// +build create_drop_db

package cli

import (
	"flag"
	"github.com/golang-migrate/migrate/v4"
	"time"
)

func init() {
	dropDB = func(migraterErr error, migrater *migrate.Migrate, startTime time.Time) {
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}
		if flag.Arg(1) == "" {
			log.fatal("error: please specify database name")
		}
		dropDBCmd(migrater, flag.Arg(1))
		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}
	}

	createDB = func(migraterErr error, migrater *migrate.Migrate, startTime time.Time) {
		if migraterErr != nil {
			log.fatalErr(migraterErr)
		}
		if flag.Arg(1) == "" {
			log.fatal("error: please specify database name")
		}
		createDBCmd(migrater, flag.Arg(1))
		if log.verbose {
			log.Println("Finished after", time.Since(startTime))
		}
	}

}
