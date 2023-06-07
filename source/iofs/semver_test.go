package iofs_test

import (
	"errors"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

func TestSemVer(t *testing.T) {
	// reuse the embed.FS set in example_test.go
	d, err := iofs.New(semverfs, "testdata/semvers")
	if err != nil {
		t.Fatal(err)
	}

	verify(t, d)
}

func verify(t *testing.T, d source.Driver) {
	verifyFirst(t, d)
	verifyPrev(t, d)
	verifyNext(t, d)
	verifyReadUp(t, d)
	verifyReadDown(t, d)
}

func verifyFirst(t *testing.T, d source.Driver) {
	version, err := d.First()
	if err != nil {
		t.Fatalf("First: expected err to be nil, got %v", err)
	}
	if version != 10000999 {
		t.Errorf("First: expected 10000999, got %v", version)
	}
}

func verifyPrev(t *testing.T, d source.Driver) {
	tt := []struct {
		version           uint
		expectErr         error
		expectPrevVersion uint
	}{
		{version: 10000999, expectErr: os.ErrNotExist},
		{version: 10100059, expectErr: nil, expectPrevVersion: 10000999},
		{version: 10100999, expectErr: nil, expectPrevVersion: 10100059},
	}

	for i, v := range tt {
		pv, err := d.Prev(v.version)
		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) &&
			v.expectErr != err {
			t.Errorf("Prev: expected %v, got %v, in %v", v.expectErr, err, i)
		}
		if err == nil && v.expectPrevVersion != pv {
			t.Errorf("Prev: expected %v, got %v, in %v", v.expectPrevVersion, pv, i)
		}
	}
}

func verifyNext(t *testing.T, d source.Driver) {
	tt := []struct {
		version           uint
		expectErr         error
		expectNextVersion uint
	}{
		{version: 0, expectErr: os.ErrNotExist},
		{version: 10000999, expectErr: nil, expectNextVersion: 10100059},
		{version: 10100059, expectErr: nil, expectNextVersion: 10100999},
	}

	for i, v := range tt {
		nv, err := d.Next(v.version)
		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) &&
			v.expectErr != err {
			t.Errorf("Next: expected %v, got %v, in %v", v.expectErr, err, i)
		}
		if err == nil && v.expectNextVersion != nv {
			t.Errorf("Next: expected %v, got %v, in %v", v.expectNextVersion, nv, i)
		}
	}
}

func verifyReadUp(t *testing.T, d source.Driver) {
	tt := []struct {
		version   uint
		expectErr error
		expectUp  bool
	}{
		{version: 0, expectErr: os.ErrNotExist},
		{version: 10100059, expectErr: nil, expectUp: true},
		{version: 10000999, expectErr: nil, expectUp: true},
	}

	for i, v := range tt {
		up, identifier, err := d.ReadUp(v.version)
		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && err != v.expectErr) {
			t.Errorf("expected %v, got %v, in %v", v.expectErr, err, i)
		} else if err == nil {
			if len(identifier) == 0 {
				t.Errorf("expected identifier not to be empty, in %v", i)
			}

			if v.expectUp && up == nil {
				t.Errorf("expected up not to be nil, in %v", i)
			} else if !v.expectUp && up != nil {
				t.Errorf("expected up to be nil, got %v, in %v", up, i)
			}
		}
		if up != nil {
			if err := up.Close(); err != nil {
				t.Error(err)
			}
		}
	}
}

func verifyReadDown(t *testing.T, d source.Driver) {
	tt := []struct {
		version    uint
		expectErr  error
		expectDown bool
	}{
		{version: 0, expectErr: os.ErrNotExist},
		{version: 10100059, expectErr: nil, expectDown: true},
		{version: 10100999, expectErr: nil, expectDown: true},
	}

	for i, v := range tt {
		down, identifier, err := d.ReadDown(v.version)
		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && err != v.expectErr) {
			t.Errorf("expected %v, got %v, in %v", v.expectErr, err, i)
		} else if err == nil {
			if len(identifier) == 0 {
				t.Errorf("expected identifier not to be empty, in %v", i)
			}

			if v.expectDown && down == nil {
				t.Errorf("expected down not to be nil, in %v", i)
			} else if !v.expectDown && down != nil {
				t.Errorf("expected down to be nil, got %v, in %v", down, i)
			}
		}
		if down != nil {
			if err := down.Close(); err != nil {
				t.Error(err)
			}
		}
	}
}
