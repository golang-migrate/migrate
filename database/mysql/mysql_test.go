package mysql

import (
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	// "io/ioutil"
	// "log"
	"testing"

	// "github.com/go-sql-driver/mysql"
	dt "github.com/mattes/migrate/database/testing"
	mt "github.com/mattes/migrate/testing"
)

var versions = []mt.Version{
	{"mysql:8", []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
	{"mysql:5.7", []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
	{"mysql:5.6", []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
	{"mysql:5.5", []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=public"}},
}

func isReady(i mt.Instance) bool {
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%v:%v)/public", i.Host(), i.Port()))
	if err != nil {
		return false
	}
	defer db.Close()
	err = db.Ping()

	if err == sqldriver.ErrBadConn {
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
