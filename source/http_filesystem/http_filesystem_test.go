package httpfs

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func Test(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// write files that meet driver test requirements
	mustWriteFile(t, tmpDir, "1_foobar.up.sql", "1 up")
	mustWriteFile(t, tmpDir, "1_foobar.down.sql", "1 down")

	mustWriteFile(t, tmpDir, "3_foobar.up.sql", "3 up")

	mustWriteFile(t, tmpDir, "4_foobar.up.sql", "4 up")
	mustWriteFile(t, tmpDir, "4_foobar.down.sql", "4 down")

	mustWriteFile(t, tmpDir, "5_foobar.down.sql", "5 down")

	mustWriteFile(t, tmpDir, "7_foobar.up.sql", "7 up")
	mustWriteFile(t, tmpDir, "7_foobar.down.sql", "7 down")

	d, err := WithInstance(http.Dir(tmpDir), &Config{})
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

func TestOpen(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "TestOpen")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	mustWriteFile(t, tmpDir, "1_foobar.up.sql", "")
	mustWriteFile(t, tmpDir, "1_foobar.down.sql", "")

	if !filepath.IsAbs(tmpDir) {
		t.Fatal("expected tmpDir to be absolute path")
	}

	_, err = WithInstance(http.Dir(tmpDir), &Config{}) // absolute path
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenWithRelativePath(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "TestOpen")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd) // rescue working dir after we are done

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(filepath.Join(tmpDir, "foo"), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mustWriteFile(t, filepath.Join(tmpDir, "foo"), "1_foobar.up.sql", "")

	// dir: foo
	d, err := WithInstance(http.Dir("foo"), &Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.First()
	if err != nil {
		t.Fatalf("expected first file in working dir %v for foo", tmpDir)
	}

	// dir: ./foo
	d, err = WithInstance(http.Dir("./foo"), &Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.First()
	if err != nil {
		t.Fatalf("expected first file in working dir %v for ./foo", tmpDir)
	}
}

func TestOpenWithDuplicateVersion(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "TestOpenWithDuplicateVersion")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	mustWriteFile(t, tmpDir, "1_foo.up.sql", "") // 1 up
	mustWriteFile(t, tmpDir, "1_bar.up.sql", "") // 1 up

	_, err = WithInstance(http.Dir(tmpDir), &Config{})
	if err == nil {
		t.Fatal("expected err")
	}
}

func TestClose(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "TestOpen")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := WithInstance(http.Dir(tmpDir), &Config{})
	if err != nil {
		t.Fatal(err)
	}

	if d.Close() != nil {
		t.Fatal("expected nil")
	}
}

func mustWriteFile(t testing.TB, dir, file string, body string) {
	if err := ioutil.WriteFile(path.Join(dir, file), []byte(body), 06444); err != nil {
		t.Fatal(err)
	}
}

func mustCreateBenchmarkDir(t *testing.B) (dir string) {
	tmpDir, err := ioutil.TempDir("", "Benchmark")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		mustWriteFile(t, tmpDir, fmt.Sprintf("%v_foobar.up.sql", i), "")
		mustWriteFile(t, tmpDir, fmt.Sprintf("%v_foobar.down.sql", i), "")
	}

	return tmpDir
}

func BenchmarkOpen(b *testing.B) {
	dir := mustCreateBenchmarkDir(b)
	defer os.RemoveAll(dir)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		WithInstance(http.Dir(dir), &Config{})
	}
	b.StopTimer()
}

func BenchmarkNext(b *testing.B) {
	dir := mustCreateBenchmarkDir(b)
	defer os.RemoveAll(dir)
	d, err := WithInstance(http.Dir(dir), &Config{})
	b.ResetTimer()
	v, err := d.First()
	for n := 0; n < b.N; n++ {
		for !os.IsNotExist(err) {
			v, err = d.Next(v)
		}
	}
	b.StopTimer()
}
