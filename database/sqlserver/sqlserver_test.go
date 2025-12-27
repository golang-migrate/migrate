package sqlserver

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 1433
const saPassword = "Root1234"

var (
	sqlServerOpts = dktest.Options{
		Env:          map[string]string{"ACCEPT_EULA": "Y", "MSSQL_SA_PASSWORD": saPassword, "MSSQL_PID": "Express"},
		PortRequired: true, ReadyFunc: isReady, PullTimeout: 2 * time.Minute,
	}
	// Container versions: https://mcr.microsoft.com/v2/mssql/server/tags/list
	specs = []dktesting.ContainerSpec{
		{ImageName: "mcr.microsoft.com/mssql/server:2022-latest", Options: sqlServerOpts},
		{ImageName: "mcr.microsoft.com/mssql/server:2019-latest", Options: sqlServerOpts},
		// Add back support for 2017 version once the image is fixed: https://github.com/microsoft/mssql-docker/issues/899
		// {ImageName: "mcr.microsoft.com/mssql/server:2017-latest", Options: sqlServerOpts},
	}
)

func msConnectionString(host, port string) string {
	return fmt.Sprintf("sqlserver://sa:%v@%v:%v?database=master", saPassword, host, port)
}

func msConnectionStringMsiWithPassword(host, port string, useMsi bool) string {
	return fmt.Sprintf("sqlserver://sa:%v@%v:%v?database=master&useMsi=%t", saPassword, host, port, useMsi)
}

func msConnectionStringMsi(host, port string, useMsi bool) string {
	return fmt.Sprintf("sqlserver://sa@%v:%v?database=master&useMsi=%t", host, port, useMsi)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}
	uri := msConnectionString(ip, port)
	db, err := sql.Open("sqlserver", uri)
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}

	return true
}

func SkipIfUnsupportedArch(t *testing.T, c dktest.ContainerInfo) {
	if strings.Contains(c.ImageName, "mssql") && !strings.HasPrefix(runtime.GOARCH, "amd") {
		t.Skipf("Image %s is not supported on arch %s", c.ImageName, runtime.GOARCH)
	}
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testMultiStatement", testMultiStatement)
	t.Run("testErrorParsing", testErrorParsing)
	t.Run("testLockWorks", testLockWorks)
	t.Run("testMsiTrue", testMsiTrue)
	t.Run("testOpenWithPasswordAndMSI", testOpenWithPasswordAndMSI)
	t.Run("testMsiFalse", testMsiFalse)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionString(ip, port)
		p := &SQLServer{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionString(ip, port)
		p := &SQLServer{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "master", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testMultiStatement(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionString(ip, port)
		ms := &SQLServer{}
		d, err := ms.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var exists int
		if err := d.(*SQLServer).conn.QueryRowContext(context.Background(), "SELECT COUNT(1) FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT schema_name()) AND table_catalog = (SELECT db_name())").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != 1 {
			t.Fatalf("expected table bar to exist")
		}
	})
}

func testErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionString(ip, port)

		p := &SQLServer{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		wantErr := `migration failed: Unknown object type 'TABLEE' used in a CREATE, DROP, or ALTER statement. in line 1:` +
			` CREATE TABLE foo (foo text); CREATE TABLEE bar (bar text); (details: mssql: Unknown object type ` +
			`'TABLEE' used in a CREATE, DROP, or ALTER statement.)`
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLEE bar (bar text);")); err == nil {
			t.Fatal("expected err but got nil")
		} else if err.Error() != wantErr {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}
	})
}

func testLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("sqlserver://sa:%v@%v:%v?master", saPassword, ip, port)
		p := &SQLServer{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}
		dt.Test(t, d, []byte("SELECT 1"))

		ms := d.(*SQLServer)

		err = ms.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = ms.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		// make sure the 2nd lock works (RELEASE_LOCK is very finicky)
		err = ms.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = ms.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func testMsiTrue(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionStringMsi(ip, port, true)
		p := &SQLServer{}
		_, err = p.Open(addr)
		if err == nil {
			t.Fatal("MSI should fail when not running in an Azure context.")
		}
	})
}

func testOpenWithPasswordAndMSI(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionStringMsiWithPassword(ip, port, true)
		p := &SQLServer{}
		_, err = p.Open(addr)
		if err == nil {
			t.Fatal("Open should fail when both password and useMsi=true are passed.")
		}

		addr = msConnectionStringMsiWithPassword(ip, port, false)
		p = &SQLServer{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func testMsiFalse(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		SkipIfUnsupportedArch(t, c)
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := msConnectionStringMsi(ip, port, false)
		p := &SQLServer{}
		_, err = p.Open(addr)
		if err == nil {
			t.Fatal("Open should fail since no password was passed and useMsi is false.")
		}
	})
}
