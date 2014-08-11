package main

import (
	"flag"
	"fmt"
	"github.com/mattes/migrate/migrate"
	"os"
	"strconv"
)

var url = flag.String("url", "schema://url", "Driver connection URL")
var path = flag.String("path", "", "Path to migrations")

func main() {
	flag.Parse()

	command := flag.Arg(0)

	if *path == "" {
		fmt.Println("Please specify path")
		os.Exit(1)
	}

	switch command {
	case "create":
		createCmd(*url, *path, flag.Arg(1))

	case "migrate":
		relativeN := flag.Arg(1)
		relativeNInt, err := strconv.Atoi(relativeN)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		migrateCmd(*url, *path, relativeNInt)

	case "help":
		helpCmd()

	}

	// fmt.Println(*url)

}

func helpCmd() {
	fmt.Fprint(os.Stderr, "Usage of migrate:\n")
	flag.PrintDefaults()
}

func createCmd(url, migrationsPath, name string) {
	files, err := migrate.Create(url, migrationsPath, name)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(files)
}

func upCmd(url, migrationsPath string) {

}

func downCmd(url, migrationsPath string) {

}

func redoCmd(url, migrationsPath string) {

}

func resetCmd(url, migrationsPath string) {

}

func versionCmd(url, migrationsPath string) {

}

func migrateCmd(url, migrationsPath string, relativeN int) {
	if err := migrate.Migrate(url, migrationsPath, relativeN); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
