package database

import (
	"io"
	"testing"
)

func ExampleDriver() {
	// see database/stub for an example

	// database/stub/stub.go has the driver implementation
	// database/stub/stub_test.go runs database/testing/test.go:Test
}

// Using database/stub here is not possible as it
// results in an import cycle.
type mockDriver struct {
	url string
}

func (m *mockDriver) Open(url string) (Driver, error) {
	return &mockDriver{
		url: url,
	}, nil
}

func (m *mockDriver) Close() error {
	return nil
}

func (m *mockDriver) Lock() error {
	return nil
}

func (m *mockDriver) Unlock() error {
	return nil
}

func (m *mockDriver) Run(migration io.Reader) error {
	return nil
}

func (m *mockDriver) SetVersion(version int, dirty bool) error {
	return nil
}

func (m *mockDriver) Version() (version int, dirty bool, err error) {
	return 0, false, nil
}

func (m *mockDriver) Drop() error {
	return nil
}

func TestRegisterTwice(t *testing.T) {
	Register("mock", &mockDriver{})

	var err interface{}
	func() {
		defer func() {
			err = recover()
		}()
		Register("mock", &mockDriver{})
	}()

	if err == nil {
		t.Fatal("expected a panic when calling Register twice")
	}
}

func TestOpen(t *testing.T) {
	// Make sure the driver is registered.
	// But if the previous test already registered it just ignore the panic.
	// If we don't do this it will be impossible to run this test standalone.
	func() {
		defer func() {
			_ = recover()
		}()
		Register("mock", &mockDriver{})
	}()

	u := "mock://user:pass@tcp(host:1337)/db"

	if d, err := Open(u); err != nil {
		t.Fatalf("did not expect %q", err)
	} else if md, ok := d.(*mockDriver); !ok {
		t.Fatalf("expected *stub.Stub got %T", d)
	} else if md.url != u {
		t.Fatalf("expected %q got %q", u, md.url)
	}

	if _, err := Open("unknown://bla"); err == nil {
		t.Fatal("expected an error for an unknown driver")
	}
}
