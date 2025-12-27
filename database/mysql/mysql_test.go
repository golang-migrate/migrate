package mysql

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/dhui/dktest"
	"github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
)

const defaultPort = 3306

var (
	opts = dktest.Options{
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "public"},
		PortRequired: true, ReadyFunc: isReady,
	}
	optsAnsiQuotes = dktest.Options{
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "public"},
		PortRequired: true, ReadyFunc: isReady,
		Cmd: []string{"--sql-mode=ANSI_QUOTES"},
	}
	// Supported versions: https://www.mysql.com/support/supportedplatforms/database.html
	specs = []dktesting.ContainerSpec{
		{ImageName: "mysql:8.0", Options: opts},
		{ImageName: "mysql:8.4", Options: opts},
		{ImageName: "mysql:9.0", Options: opts},
	}
	specsAnsiQuotes = []dktesting.ContainerSpec{
		{ImageName: "mysql:8.0", Options: optsAnsiQuotes},
		{ImageName: "mysql:8.4", Options: optsAnsiQuotes},
		{ImageName: "mysql:9.0", Options: optsAnsiQuotes},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%v:%v)/public", ip, port))
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
		case sqldriver.ErrBadConn, mysql.ErrInvalidConn:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
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

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrate(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrateAnsiQuotes(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specsAnsiQuotes, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))

		ms := d.(*Mysql)

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

func TestNoLockParamValidation(t *testing.T) {
	ip := "127.0.0.1"
	port := 3306
	addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
	p := &Mysql{}
	_, err := p.Open(addr + "?x-no-lock=not-a-bool")
	if !errors.Is(err, strconv.ErrSyntax) {
		t.Fatal("Expected syntax error when passing a non-bool as x-no-lock parameter")
	}
}

func TestNoLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		lock := d.(*Mysql)

		p = &Mysql{}
		d, err = p.Open(addr + "?x-no-lock=true")
		if err != nil {
			t.Fatal(err)
		}

		noLock := d.(*Mysql)

		// Should be possible to take real lock and no-lock at the same time
		if err = lock.Lock(); err != nil {
			t.Fatal(err)
		}
		if err = noLock.Lock(); err != nil {
			t.Fatal(err)
		}
		if err = lock.Unlock(); err != nil {
			t.Fatal(err)
		}
		if err = noLock.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExtractCustomQueryParams(t *testing.T) {
	testcases := []struct {
		name                 string
		config               *mysql.Config
		expectedParams       map[string]string
		expectedCustomParams map[string]string
		expectedErr          error
	}{
		{name: "nil config", expectedErr: ErrNilConfig},
		{
			name:                 "no params",
			config:               mysql.NewConfig(),
			expectedCustomParams: map[string]string{},
		},
		{
			name:                 "no custom params",
			config:               &mysql.Config{Params: map[string]string{"hello": "world"}},
			expectedParams:       map[string]string{"hello": "world"},
			expectedCustomParams: map[string]string{},
		},
		{
			name: "one param, one custom param",
			config: &mysql.Config{
				Params: map[string]string{"hello": "world", "x-foo": "bar"},
			},
			expectedParams:       map[string]string{"hello": "world"},
			expectedCustomParams: map[string]string{"x-foo": "bar"},
		},
		{
			name: "multiple params, multiple custom params",
			config: &mysql.Config{
				Params: map[string]string{
					"hello": "world",
					"x-foo": "bar",
					"dead":  "beef",
					"x-cat": "hat",
				},
			},
			expectedParams:       map[string]string{"hello": "world", "dead": "beef"},
			expectedCustomParams: map[string]string{"x-foo": "bar", "x-cat": "hat"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			customParams, err := extractCustomQueryParams(tc.config)
			if tc.config != nil {
				assert.Equal(t, tc.expectedParams, tc.config.Params,
					"Expected config params have custom params properly removed")
			}
			assert.Equal(t, tc.expectedErr, err, "Expected errors to match")
			assert.Equal(t, tc.expectedCustomParams, customParams,
				"Expected custom params to be properly extracted")
		})
	}
}

func createTmpCert(t *testing.T) string {
	tmpCertFile, err := os.CreateTemp("", "migrate_test_cert")
	if err != nil {
		t.Fatal("Failed to create temp cert file:", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(tmpCertFile.Name()); err != nil {
			t.Log("Failed to cleanup temp cert file:", err)
		}
	})

	r := rand.New(rand.NewSource(0))
	pub, priv, err := ed25519.GenerateKey(r)
	if err != nil {
		t.Fatal("Failed to generate ed25519 key for temp cert file:", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(0),
	}
	derBytes, err := x509.CreateCertificate(r, &tmpl, &tmpl, pub, priv)
	if err != nil {
		t.Fatal("Failed to generate temp cert file:", err)
	}
	if err := pem.Encode(tmpCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatal("Failed to encode ")
	}
	if err := tmpCertFile.Close(); err != nil {
		t.Fatal("Failed to close temp cert file:", err)
	}
	return tmpCertFile.Name()
}

func TestURLToMySQLConfig(t *testing.T) {
	tmpCertFilename := createTmpCert(t)
	tmpCertFilenameEscaped := url.PathEscape(tmpCertFilename)

	testcases := []struct {
		name        string
		urlStr      string
		expectedDSN string // empty string signifies that an error is expected
	}{
		{name: "no user/password", urlStr: "mysql://tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user", urlStr: "mysql://username@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user - with encoded :",
			urlStr:      "mysql://username%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user - with encoded @",
			urlStr:      "mysql://username%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password", urlStr: "mysql://username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		// Not supported yet: https://github.com/go-sql-driver/mysql/issues/591
		// {name: "user/password - user with encoded :",
		// 	urlStr:      "mysql://username%3A:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
		// 	expectedDSN: "username::password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - user with encoded @",
			urlStr:      "mysql://username%40:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded :",
			urlStr:      "mysql://username:password%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded @",
			urlStr:      "mysql://username:password%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "custom tls",
			urlStr:      "mysql://username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true&tls=custom&x-tls-ca=" + tmpCertFilenameEscaped,
			expectedDSN: "username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true&tls=custom&x-tls-ca=" + tmpCertFilenameEscaped},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := urlToMySQLConfig(tc.urlStr)
			if err != nil {
				t.Fatal("Failed to parse url string:", tc.urlStr, "error:", err)
			}
			dsn := config.FormatDSN()
			if dsn != tc.expectedDSN {
				t.Error("Got unexpected DSN:", dsn, "!=", tc.expectedDSN)
			}
		})
	}
}
