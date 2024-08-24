package reflect

import (
	"io"
	"log"
	"testing"

	st "github.com/golang-migrate/migrate/source/testing"
)

func TestExample(t *testing.T) {
	migrations := &struct {
		Users_up   string // v1
		Users_down string // v1
		Folders_up string // v2
		Posts_up   string // v3
		Posts_down string // v3
	}{

		Users_up: `
			create table users (
			id int not null primary key,
			created_at timestamp not null,
			email text not null,
			password text not null);`,

		Users_down: `drop table users;`,

		Folders_up: `create table folders (
			id int not null primary key,
			created_at timestamp not null,
			label text not null);`,

		Posts_up: `
			create table posts (
			id int not null primary key,
			created_at timestamp not null,
			user_id int not null,
			body text not null);`,

		Posts_down: `drop table posts;`,
	}

	driver, err := New(migrations)
	if err != nil {
		log.Fatal(err)
	}

	if driver == nil {
		log.Fatal("driver should not be nil")
	}

	rdr, label, err := driver.ReadUp(3)
	if err != nil {
		log.Fatal(err)
	}

	txt, err := io.ReadAll(rdr)
	if err != nil {
		log.Fatal(err)
	}

	if string(txt) != migrations.Posts_up {
		log.Fatal("unexpected text: " + string(txt))
	}

	if label != "Posts" {
		log.Fatal("unexpected label")
	}

	rdr, label, err = driver.ReadDown(3)
	if err != nil {
		log.Fatal(err)
	}

	txt, err = io.ReadAll(rdr)
	if err != nil {
		log.Fatal(err)
	}

	if string(txt) != migrations.Posts_down {
		log.Fatal("unexpected text: " + string(txt))
	}

	if label != "Posts" {
		log.Fatal("unexpected label")
	}

}

func Test(t *testing.T) {
	migrations := &struct {
		Table1_up   string `migrate:"1"`
		Table1_down string `migrate:"1"`
		Table2_up   string `migrate:"3"`
		Table3_up   string `migrate:"4"`
		Table3_down string `migrate:"4"`
		Table4_down string `migrate:"5"`
		Table5_down string `migrate:"7"`
		Table5_up   string `migrate:"7"`
	}{
		Table1_up:   `test statement 1`,
		Table1_down: `test statement 2`,
		Table2_up:   `test statement 3`,
		Table3_up:   `test statement 4`,
		Table3_down: `test statement 5`,
		Table4_down: `test statement 6`,
		Table5_up:   `test statement 7`,
		Table5_down: `test statement 8`,
	}

	driver, err := New(migrations)
	if err != nil {
		log.Fatal(err)
	}

	st.Test(t, driver)
}
