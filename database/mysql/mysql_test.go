package mysql

import (
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"net/url"
	"testing"
)

import (
	"github.com/go-sql-driver/mysql"
)

import (
	dt "github.com/golang-migrate/migrate/database/testing"
	mt "github.com/golang-migrate/migrate/testing"
)

var versions = []mt.Version{
	{Image: "mysql:8", ENV: []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
	{Image: "mysql:5.7", ENV: []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
	{Image: "mysql:5.6", ENV: []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
	{Image: "mysql:5.5", ENV: []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
}

func isReady(i mt.Instance) bool {
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%v:%v)/public", i.Host(), i.Port()))
	if err != nil {
		return false
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
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
	// mysql.SetLogger(mysql.Logger(log.New(ioutil.Discard, "", log.Ltime)))

	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Mysql{}
			addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()
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

func TestLockWorks(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Mysql{}
			addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
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

func TestURLToMySQLConfig(t *testing.T) {
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
		// 	expectedDSN: "username::pasword@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - user with encoded @",
			urlStr:      "mysql://username%40:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded :",
			urlStr:      "mysql://username:password%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded @",
			urlStr:      "mysql://username:password%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.urlStr)
			if err != nil {
				t.Fatal("Failed to parse url string:", tc.urlStr, "error:", err)
			}
			if config, err := urlToMySQLConfig(*u); err == nil {
				dsn := config.FormatDSN()
				if dsn != tc.expectedDSN {
					t.Error("Got unexpected DSN:", dsn, "!=", tc.expectedDSN)
				}
			} else {
				if tc.expectedDSN != "" {
					t.Error("Got unexpected error:", err, "urlStr:", tc.urlStr)
				}
			}
		})
	}
}
