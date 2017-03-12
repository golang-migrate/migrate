package main

import (
	"github.com/mattes/migrate"
	_ "github.com/mattes/migrate/database/stub" // TODO remove again
	_ "github.com/mattes/migrate/source/file"
)

func gotoCmd(m *migrate.Migrate, v uint) {
	if err := m.Migrate(v); err != nil {
		if err != migrate.ErrNoChange {
			log.fatalErr(err)
		} else {
			log.Println(err)
		}
	}
}

func upCmd(m *migrate.Migrate, limit int) {
	if limit >= 0 {
		if err := m.Steps(limit); err != nil {
			if err != migrate.ErrNoChange {
				log.fatalErr(err)
			} else {
				log.Println(err)
			}
		}
	} else {
		if err := m.Up(); err != nil {
			if err != migrate.ErrNoChange {
				log.fatalErr(err)
			} else {
				log.Println(err)
			}
		}
	}
}

func downCmd(m *migrate.Migrate, limit int) {
	if limit >= 0 {
		if err := m.Steps(-limit); err != nil {
			if err != migrate.ErrNoChange {
				log.fatalErr(err)
			} else {
				log.Println(err)
			}
		}
	} else {
		if err := m.Down(); err != nil {
			if err != migrate.ErrNoChange {
				log.fatalErr(err)
			} else {
				log.Println(err)
			}
		}
	}
}

func dropCmd(m *migrate.Migrate) {
	if err := m.Drop(); err != nil {
		log.fatalErr(err)
	}
}

func forceCmd(m *migrate.Migrate, v int) {
	if err := m.Force(v); err != nil {
		log.fatalErr(err)
	}
}

func versionCmd(m *migrate.Migrate) {
	v, dirty, err := m.Version()
	if err != nil {
		log.fatalErr(err)
	}
	if dirty {
		log.Printf("%v (dirty)\n", v)
	} else {
		log.Println(v)
	}
}
