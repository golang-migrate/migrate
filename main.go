package main

import (
	"flag"
	"fmt"
	"github.com/mattes/migrate/migrate"
	"os"
)

var db = flag.String("db", "schema://url", "Driver connection URL")
var path = flag.String("path", "./db/migrations:./migrations:./db", "Migrations search path")
var help = flag.Bool("help", false, "Show help")

func main() {
	flag.Parse()

	if *help {
		usage()
		os.Exit(0)
	}

	command := flag.Arg(0)
	switch command {
	case "create":
		if *path != "" {
			migrate.SetSearchPath(*path)
		}
		files, err := migrate.Create(*db, "blablabla")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(files)
	}

	// fmt.Println(*db)

}

func usage() {
	fmt.Fprint(os.Stderr, "Usage of migrate:\n")
	flag.PrintDefaults()
}
