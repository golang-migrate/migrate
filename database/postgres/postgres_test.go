package postgres

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	dt "github.com/golang-migrate/migrate/database/testing"
	mt "github.com/golang-migrate/migrate/testing"
)

var versions = []mt.Version{
	{Image: "postgres:10"},
	{Image: "postgres:9.6"},
	{Image: "postgres:9.5"},
	{Image: "postgres:9.4"},
	{Image: "postgres:9.3"},
}

func pgConnectionString(host string, port uint) string {
	return fmt.Sprintf("postgres://postgres@%s:%v/postgres?sslmode=disable", host, port)
}

func isReady(i mt.Instance) bool {
	db, err := sql.Open("postgres", pgConnectionString(i.Host(), i.Port()))
	if err != nil {
		return false
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Postgres{}
			addr := pgConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()
			dt.Test(t, d, []byte("SELECT 1"))
		})
}

func TestMultiStatement(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Postgres{}
			addr := pgConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()
			if err := d.Run(bytes.NewReader([]byte("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);"))); err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			// make sure second table exists
			var exists bool
			if err := d.(*Postgres).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				t.Fatalf("expected table bar to exist")
			}
		})
}

func TestErrorParsing(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Postgres{}
			addr := pgConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()

			wantErr := `migration failed: syntax error at or near "TABLEE" (column 37) in line 1: CREATE TABLE foo ` +
				`(foo text); CREATE TABLEE bar (bar text); (details: pq: syntax error at or near "TABLEE")`
			if err := d.Run(bytes.NewReader([]byte("CREATE TABLE foo (foo text); CREATE TABLEE bar (bar text);"))); err == nil {
				t.Fatal("expected err but got nil")
			} else if err.Error() != wantErr {
				t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
			}
		})
}

func TestFilterCustomQuery(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Postgres{}
			addr := fmt.Sprintf("postgres://postgres@%v:%v/postgres?sslmode=disable&x-custom=foobar", i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()
		})
}

func TestWithSchema(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Postgres{}
			addr := pgConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()

			// create foobar schema
			if err := d.Run(bytes.NewReader([]byte("CREATE SCHEMA foobar AUTHORIZATION postgres"))); err != nil {
				t.Fatal(err)
			}
			if err := d.SetVersion(1, false); err != nil {
				t.Fatal(err)
			}

			// re-connect using that schema
			d2, err := p.Open(fmt.Sprintf("postgres://postgres@%v:%v/postgres?sslmode=disable&search_path=foobar", i.Host(), i.Port()))
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d2.Close()

			version, _, err := d2.Version()
			if err != nil {
				t.Fatal(err)
			}
			if version != -1 {
				t.Fatal("expected NilVersion")
			}

			// now update version and compare
			if err := d2.SetVersion(2, false); err != nil {
				t.Fatal(err)
			}
			version, _, err = d2.Version()
			if err != nil {
				t.Fatal(err)
			}
			if version != 2 {
				t.Fatal("expected version 2")
			}

			// meanwhile, the public schema still has the other version
			version, _, err = d.Version()
			if err != nil {
				t.Fatal(err)
			}
			if version != 1 {
				t.Fatal("expected version 2")
			}
		})
}

func TestWithInstance(t *testing.T) {

}

func TestPostgres_Lock(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Postgres{}
			addr := pgConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}

			dt.Test(t, d, []byte("SELECT 1"))

			ps := d.(*Postgres)

			err = ps.Lock()
			if err != nil {
				t.Fatal(err)
			}

			err = ps.Unlock()
			if err != nil {
				t.Fatal(err)
			}

			err = ps.Lock()
			if err != nil {
				t.Fatal(err)
			}

			err = ps.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		})
}

func Test_computeLineFromPos(t *testing.T) {
	testcases := []struct {
		pos      int
		wantLine uint
		wantCol  uint
		input    string
		wantOk   bool
	}{
		{
			15, 2, 6, "SELECT *\nFROM foo", true, // foo table does not exists
		},
		{
			16, 3, 6, "SELECT *\n\nFROM foo", true, // foo table does not exists, empty line
		},
		{
			25, 3, 7, "SELECT *\nFROM foo\nWHERE x", true, // x column error
		},
		{
			27, 5, 7, "SELECT *\n\nFROM foo\n\nWHERE x", true, // x column error, empty lines
		},
		{
			10, 2, 1, "SELECT *\nFROMM foo", true, // FROMM typo
		},
		{
			11, 3, 1, "SELECT *\n\nFROMM foo", true, // FROMM typo, empty line
		},
		{
			17, 2, 8, "SELECT *\nFROM foo", true, // last character
		},
		{
			18, 0, 0, "SELECT *\nFROM foo", false, // invalid position
		},
	}
	for i, tc := range testcases {
		t.Run("tc"+strconv.Itoa(i), func(t *testing.T) {
			run := func(crlf bool, nonASCII bool) {
				var name string
				if crlf {
					name = "crlf"
				} else {
					name = "lf"
				}
				if nonASCII {
					name += "-nonascii"
				} else {
					name += "-ascii"
				}
				t.Run(name, func(t *testing.T) {
					input := tc.input
					if crlf {
						input = strings.Replace(input, "\n", "\r\n", -1)
					}
					if nonASCII {
						input = strings.Replace(input, "FROM", "FRÖM", -1)
					}
					gotLine, gotCol, gotOK := computeLineFromPos(input, tc.pos)

					if tc.wantOk {
						t.Logf("pos %d, want %d:%d, %#v", tc.pos, tc.wantLine, tc.wantCol, input)
					}

					if gotOK != tc.wantOk {
						t.Fatalf("expected ok %v but got %v", tc.wantOk, gotOK)
					}
					if gotLine != tc.wantLine {
						t.Fatalf("expected line %d but got %d", tc.wantLine, gotLine)
					}
					if gotCol != tc.wantCol {
						t.Fatalf("expected col %d but got %d", tc.wantCol, gotCol)
					}
				})
			}
			run(false, false)
			run(true, false)
			run(false, true)
			run(true, true)
		})
	}

}
