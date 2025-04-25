package fn

import (
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

var (
	execFunc  = source.ExecutorFunc(func(i interface{}) error { return nil })
	migrators = map[string]*Migration{
		"1_test": {
			Up:   execFunc,
			Down: execFunc,
		},
		"3_test": {
			Up: execFunc,
		},
		"4_test": {
			Up:   execFunc,
			Down: execFunc,
		},
		"5_test": {
			Down: execFunc,
		},
		"7_test": {
			Up:   execFunc,
			Down: execFunc,
		},
	}
)

func Test(t *testing.T) {
	d, err := WithInstance(migrators)
	if err != nil {
		t.Fatal(err)
	}
	st.Test(t, d)
}

func TestWithInstance(t *testing.T) {
	_, err := WithInstance(migrators)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	b := &Func{}
	_, err := b.Open("")
	if err == nil {
		t.Fatal("expected err, because it's not implemented yet")
	}
}
