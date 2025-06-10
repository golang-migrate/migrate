package embed

import (
	"embed"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

//go:embed testmigrations/*.sql
var testFS embed.FS

const testPath = "testmigrations"

func Test(t *testing.T) {
	driver, err := NewEmbed(testFS, testPath)
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, driver)
}

func TestNewEmbed_Success(t *testing.T) {
	driver, err := NewEmbed(testFS, testPath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if driver == nil {
		t.Fatal("expected driver, got nil")
	}
}

func TestNewEmbed_InvalidPath(t *testing.T) {
	_, err := NewEmbed(testFS, "doesnotexist")
	if err == nil {
		t.Fatal("expected error for invalid path, got nil")
	}
}

func TestEmbed_Open(t *testing.T) {
	driver, _ := NewEmbed(testFS, "testmigrations")
	_, err := driver.(*Embed).Open("someurl")
	if err == nil || err.Error() != "Open() cannot be called on the embed driver" {
		t.Fatalf("expected Open() error, got %v", err)
	}
}

func TestEmbed_First(t *testing.T) {
	driver, _ := NewEmbed(testFS, "testmigrations")
	version, err := driver.First()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if version == 0 {
		t.Fatal("expected non-zero version")
	}
}

func TestEmbed_First_Empty(t *testing.T) {
	emptyFS := embed.FS{}
	e := &Embed{}
	e.FS = emptyFS
	e.path = "empty"
	e.migrations = source.NewMigrations()
	_, err := e.First()
	if err == nil {
		t.Fatal("expected error for empty migrations")
	}
}

func TestEmbed_PrevNext(t *testing.T) {
	driver, _ := NewEmbed(testFS, "testmigrations")
	first, _ := driver.First()
	_, err := driver.Prev(first)
	if err == nil {
		t.Fatal("expected error for prev of first migration")
	}
	next, err := driver.Next(first)
	if err != nil {
		t.Fatalf("expected no error for next, got %v", err)
	}
	if next == 0 {
		t.Fatal("expected next version to be non-zero")
	}
}

func TestEmbed_ReadUpDown(t *testing.T) {
	driver, _ := NewEmbed(testFS, "testmigrations")
	first, _ := driver.First()
	r, id, err := driver.ReadUp(first)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if r == nil || id == "" {
		t.Fatal("expected valid reader and identifier")
	}
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("expected file content")
	}
	r.Close()

	// Down migration may not exist for first, so test with next if available
	next, _ := driver.Next(first)
	rd, idd, err := driver.ReadDown(next)
	if err == nil {
		if rd == nil || idd == "" {
			t.Fatal("expected valid reader and identifier for down")
		}
		rd.Close()
	}
}

func TestEmbed_ReadUp_NotExist(t *testing.T) {
	driver, _ := NewEmbed(testFS, "testmigrations")
	_, _, err := driver.ReadUp(999999)
	if err == nil {
		t.Fatal("expected error for non-existent migration")
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("expected fs.PathError, got %T", err)
	}
}

func TestEmbed_Close(t *testing.T) {
	driver, _ := NewEmbed(testFS, "testmigrations")
	if err := driver.Close(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestFileReader_ReadClose(t *testing.T) {
	data := []byte("hello world")
	fr := &fileReader{data: data}
	buf := make([]byte, 5)
	n, err := fr.Read(buf)
	if n != 5 || err != nil {
		t.Fatalf("expected to read 5 bytes, got %d, err %v", n, err)
	}
	n, err = fr.Read(buf)
	if n != 5 || err != nil {
		t.Fatalf("expected to read next 5 bytes, got %d, err %v", n, err)
	}
	n, err = fr.Read(buf)
	if n != 1 || err != nil {
		t.Fatalf("expected to read last byte, got %d, err %v", n, err)
	}
	n, err = fr.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("expected EOF, got %d, err %v", n, err)
	}
	if err := fr.Close(); err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

// createBenchmarkEmbed creates an Embed driver with test migrations
// This is a helper function for benchmarks
func createBenchmarkEmbed(b *testing.B) *Embed {
	driver, err := NewEmbed(testFS, testPath)
	if err != nil {
		b.Fatal(err)
	}
	return driver.(*Embed)
}

func BenchmarkFirst(b *testing.B) {
	e := createBenchmarkEmbed(b)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := e.First()
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}

func BenchmarkNext(b *testing.B) {
	e := createBenchmarkEmbed(b)
	b.ResetTimer()
	v, err := e.First()
	for n := 0; n < b.N; n++ {
		for !errors.Is(err, fs.ErrNotExist) {
			v, err = e.Next(v)
		}
	}
	b.StopTimer()
}
