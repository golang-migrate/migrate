package libsql

import (
	"fmt"
	"path/filepath"
	"testing"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
)

func Test(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite.db"))
	l := &Libsql{}
	addr := fmt.Sprintf("file:%s", filepath.Join(dir, "sqlite.db"))
	d, err := l.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}
