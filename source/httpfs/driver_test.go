package httpfs

import (
	"net/http"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func TestWithInstance(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		d, err := WithInstance(http.Dir("testdata/sql"), "")
		if err != nil {
			t.Fatal(err)
		}
		st.Test(t, d)
	})

	t.Run("with path", func(t *testing.T) {
		d, err := WithInstance(http.Dir("testdata"), "sql")
		if err != nil {
			t.Fatal(err)
		}
		st.Test(t, d)
	})

}

func TestOpen(t *testing.T) {
	b := &driver{}
	d, err := b.Open("")
	if d != nil {
		t.Error("Expected Open to return nil driver")
	}
	if err == nil {
		t.Error("Expected Open to return error")
	}
}
