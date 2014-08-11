package main

import (
	"flag"
	"fmt"
	"github.com/mattes/migrate/migrate"
	"os"
	"strconv"
)

var url = flag.String("url", "", "Driver connection URL, like schema://url")
var migrationsPath = flag.String("path", "", "Path to migrations")

func main() {
	flag.Parse()
	command := flag.Arg(0)

	switch command {
	case "create":
		verifyMigrationsPath(*migrationsPath)
		createCmd(*url, *migrationsPath, flag.Arg(1))

	case "migrate":
		verifyMigrationsPath(*migrationsPath)

		relativeN := flag.Arg(1)
		relativeNInt, err := strconv.Atoi(relativeN)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		migrateCmd(*url, *migrationsPath, relativeNInt)

	case "up":
		verifyMigrationsPath(*migrationsPath)
		upCmd(*url, *migrationsPath)

	case "down":
		verifyMigrationsPath(*migrationsPath)
		downCmd(*url, *migrationsPath)

	case "redo":
		verifyMigrationsPath(*migrationsPath)
		redoCmd(*url, *migrationsPath)

	case "reset":
		verifyMigrationsPath(*migrationsPath)
		resetCmd(*url, *migrationsPath)

	case "version":
		verifyMigrationsPath(*migrationsPath)
		versionCmd(*url, *migrationsPath)

	case "help":
		helpCmd()

	default:
		helpCmd()
	}
}

func verifyMigrationsPath(path string) {
	if path == "" {
		fmt.Println("Please specify path")
		os.Exit(1)
	}
}

func writePipe(pipe chan interface{}) {
	if pipe != nil {
		for {
			select {
			case item, ok := <-pipe:
				if !ok {
					return

				} else {
					switch item.(type) {
					case string:
						fmt.Println(item.(string))
					case error:
						fmt.Println(item.(error).Error())
					default:
						fmt.Println("%v", item)
					}
				}
			}
		}
	}
}

func createCmd(url, migrationsPath, name string) {
	migrationFile, err := migrate.Create(url, migrationsPath, name)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("Version %v migration files created in %v:\n", migrationFile.Version, migrationsPath)
	fmt.Println(migrationFile.UpFile.FileName)
	fmt.Println(migrationFile.DownFile.FileName)

}

func upCmd(url, migrationsPath string) {
	writePipe(migrate.UpPipe(url, migrationsPath, nil))
}

func downCmd(url, migrationsPath string) {
	writePipe(migrate.DownPipe(url, migrationsPath, nil))
}

func redoCmd(url, migrationsPath string) {
	writePipe(migrate.RedoPipe(url, migrationsPath, nil))
}

func resetCmd(url, migrationsPath string) {
	writePipe(migrate.ResetPipe(url, migrationsPath, nil))
}

func versionCmd(url, migrationsPath string) {
	version, err := migrate.Version(url, migrationsPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(version)
}

func migrateCmd(url, migrationsPath string, relativeN int) {
	writePipe(migrate.MigratePipe(url, migrationsPath, relativeN, nil))
}

func helpCmd() {
	os.Stderr.WriteString(
		`usage: migrate [-path=<path>] [-url=<url>] <command> [<args>]

Commands:
   create <name>  Create a new migration
   up             Apply all -up- migrations
   down           Apply all -down- migrations
   reset          Down followed by Up
   redo           Roll back most recent migration, then apply it again
   version        Show current migration version
   migrate <n>    Apply migrations -n|+n
   help           Show this help
`)
}
