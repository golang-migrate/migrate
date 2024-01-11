package file

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

const scheme = "file://"

func Test(t *testing.T) {
	tmpDir := t.TempDir()

	// write files that meet driver test requirements
	mustWriteFile(t, tmpDir, "1_foobar.up.sql", "1 up")
	mustWriteFile(t, tmpDir, "1_foobar.down.sql", "1 down")

	mustWriteFile(t, tmpDir, "3_foobar.up.sql", "3 up")

	mustWriteFile(t, tmpDir, "4_foobar.up.sql", "4 up")
	mustWriteFile(t, tmpDir, "4_foobar.down.sql", "4 down")

	mustWriteFile(t, tmpDir, "5_foobar.down.sql", "5 down")

	mustWriteFile(t, tmpDir, "7_foobar.up.sql", "7 up")
	mustWriteFile(t, tmpDir, "7_foobar.down.sql", "7 down")

	f := &File{}
	d, err := f.Open(scheme + tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

func TestOpen(t *testing.T) {
	tmpDir := t.TempDir()

	mustWriteFile(t, tmpDir, "1_foobar.up.sql", "")
	mustWriteFile(t, tmpDir, "1_foobar.down.sql", "")

	if !filepath.IsAbs(tmpDir) {
		t.Fatal("expected tmpDir to be absolute path")
	}

	f := &File{}
	_, err := f.Open(scheme + tmpDir) // absolute path
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenWithRelativePath(t *testing.T) {
	tmpDir := t.TempDir()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// rescue working dir after we are done
		if err := os.Chdir(wd); err != nil {
			t.Log(err)
		}
	}()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(filepath.Join(tmpDir, "foo"), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mustWriteFile(t, filepath.Join(tmpDir, "foo"), "1_foobar.up.sql", "")

	f := &File{}

	// dir: foo
	d, err := f.Open("file://foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.First()
	if err != nil {
		t.Fatalf("expected first file in working dir %v for foo", tmpDir)
	}

	// dir: ./foo
	d, err = f.Open("file://./foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.First()
	if err != nil {
		t.Fatalf("expected first file in working dir %v for ./foo", tmpDir)
	}
}

func TestOpenDefaultsToCurrentDirectory(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	f := &File{}
	d, err := f.Open(scheme)
	if err != nil {
		t.Fatal(err)
	}

	if d.(*File).path != wd {
		t.Fatal("expected driver to default to current directory")
	}
}

func TestOpenWithDuplicateVersion(t *testing.T) {
	tmpDir := t.TempDir()

	mustWriteFile(t, tmpDir, "1_foo.up.sql", "") // 1 up
	mustWriteFile(t, tmpDir, "1_bar.up.sql", "") // 1 up

	f := &File{}
	_, err := f.Open(scheme + tmpDir)
	if err == nil {
		t.Fatal("expected err")
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()

	f := &File{}
	d, err := f.Open(scheme + tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if d.Close() != nil {
		t.Fatal("expected nil")
	}
}

func mustWriteFile(t testing.TB, dir, file string, body string) {
	if err := os.WriteFile(path.Join(dir, file), []byte(body), 06444); err != nil {
		t.Fatal(err)
	}
}

func mustCreateBenchmarkDir(t *testing.B) (dir string) {
	tmpDir := t.TempDir()

	for i := 0; i < 1000; i++ {
		mustWriteFile(t, tmpDir, fmt.Sprintf("%v_foobar.up.sql", i), "")
		mustWriteFile(t, tmpDir, fmt.Sprintf("%v_foobar.down.sql", i), "")
	}

	return tmpDir
}

func BenchmarkOpen(b *testing.B) {
	dir := mustCreateBenchmarkDir(b)
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Error(err)
		}
	}()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		f := &File{}
		_, err := f.Open(scheme + dir)
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}

func BenchmarkNext(b *testing.B) {
	dir := mustCreateBenchmarkDir(b)
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Error(err)
		}
	}()
	f := &File{}
	d, _ := f.Open(scheme + dir)
	b.ResetTimer()
	v, err := d.First()
	for n := 0; n < b.N; n++ {
		for !errors.Is(err, os.ErrNotExist) {
			v, err = d.Next(v)
		}
	}
	b.StopTimer()
}
