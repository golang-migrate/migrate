package firebird

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"io"
	"strings"
	"testing"

	"github.com/dhui/dktest"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	_ "github.com/nakagami/firebirdsql"
)

const (
	user     = "test_user"
	password = "123456"
	dbName   = "test.fdb"
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		Env: map[string]string{
			"FIREBIRD_DATABASE": dbName,
			"FIREBIRD_USER":     user,
			"FIREBIRD_PASSWORD": password,
		},
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "jacobalberty/firebird:v3.0", Options: opts},
		{ImageName: "jacobalberty/firebird:v4.0", Options: opts},
		{ImageName: "jacobalberty/firebird:v5.0", Options: opts},
	}
)

func fbConnectionString(host, port string) string {
	//firebird://user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]
	return fmt.Sprintf("firebird://%s:%s@%s:%s//firebird/data/%s", user, password, host, port, dbName)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	db, err := sql.Open("firebirdsql", fbConnectionString(ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("SELECT Count(*) FROM rdb$relations"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "firebirdsql", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		wantErr := `migration failed in line 0: CREATE TABLEE foo (foo varchar(40)); (details: Dynamic SQL Error
SQL error code = -104
Token unknown - line 1, column 8
TABLEE
)`

		if err := d.Run(strings.NewReader("CREATE TABLEE foo (foo varchar(40));")); err == nil {
			t.Fatal("expected err but got nil")
		} else if err.Error() != wantErr {
			msg := err.Error()
			t.Fatalf("expected '%s' but got '%s'", wantErr, msg)
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port) + "?sslmode=disable&x-custom=foobar"
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	})
}

func Test_Lock(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := fbConnectionString(ip, port)
		p := &Firebird{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT Count(*) FROM rdb$relations"))

		ps := d.(*Firebird)

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
