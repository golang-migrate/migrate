package source

import (
	"testing"
)

func TestNewMigrations(t *testing.T) {
	// TODO
}

func TestAppend(t *testing.T) {
	// TODO
}

func TestInterleavedAppendReads(t *testing.T) {
	m := NewMigrations()

	if !m.Append(&Migration{Version: 10, Direction: Up}) {
		t.Fatal("expected append version 10 up to succeed")
	}
	if !m.Append(&Migration{Version: 10, Direction: Down}) {
		t.Fatal("expected append version 10 down to succeed")
	}
	if !m.Append(&Migration{Version: 30, Direction: Up}) {
		t.Fatal("expected append version 30 up to succeed")
	}

	if v, ok := m.First(); !ok || v != 10 {
		t.Fatalf("expected first version 10, got %v, %v", v, ok)
	}
	if v, ok := m.Next(10); !ok || v != 30 {
		t.Fatalf("expected next version after 10 to be 30, got %v, %v", v, ok)
	}
	if v, ok := m.Prev(30); !ok || v != 10 {
		t.Fatalf("expected previous version before 30 to be 10, got %v, %v", v, ok)
	}

	if !m.Append(&Migration{Version: 20, Direction: Up}) {
		t.Fatal("expected append version 20 up to succeed")
	}
	if !m.Append(&Migration{Version: 20, Direction: Down}) {
		t.Fatal("expected append version 20 down to succeed")
	}
	if !m.Append(&Migration{Version: 5, Direction: Up}) {
		t.Fatal("expected append version 5 up to succeed")
	}

	if v, ok := m.First(); !ok || v != 5 {
		t.Fatalf("expected first version 5, got %v, %v", v, ok)
	}
	if v, ok := m.Next(10); !ok || v != 20 {
		t.Fatalf("expected next version after 10 to be 20, got %v, %v", v, ok)
	}
	if v, ok := m.Next(20); !ok || v != 30 {
		t.Fatalf("expected next version after 20 to be 30, got %v, %v", v, ok)
	}
	if v, ok := m.Prev(20); !ok || v != 10 {
		t.Fatalf("expected previous version before 20 to be 10, got %v, %v", v, ok)
	}
}

func TestBuildIndex(t *testing.T) {
	// TODO
}

func TestFirst(t *testing.T) {
	// TODO
}

func TestPrev(t *testing.T) {
	// TODO
}

func TestUp(t *testing.T) {
	// TODO
}

func TestDown(t *testing.T) {
	// TODO
}

func TestFindPos(t *testing.T) {
	m := Migrations{index: uintSlice{1, 2, 3}}
	if p := m.findPos(0); p != -1 {
		t.Errorf("expected -1, got %v", p)
	}
	if p := m.findPos(1); p != 0 {
		t.Errorf("expected 0, got %v", p)
	}
	if p := m.findPos(3); p != 2 {
		t.Errorf("expected 2, got %v", p)
	}
}

func BenchmarkAppend100(b *testing.B) {
	benchmarkAppend(b, 100)
}

func BenchmarkAppend1000(b *testing.B) {
	benchmarkAppend(b, 1000)
}

func BenchmarkAppend5000(b *testing.B) {
	benchmarkAppend(b, 5000)
}

func benchmarkAppend(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		m := NewMigrations()
		for version := 1; version <= n; version++ {
			v := uint(version)
			if !m.Append(&Migration{Version: v, Direction: Up}) {
				b.Fatalf("expected append version %d up to succeed", version)
			}
			if !m.Append(&Migration{Version: v, Direction: Down}) {
				b.Fatalf("expected append version %d down to succeed", version)
			}
		}
		if _, ok := m.First(); !ok {
			b.Fatal("expected first version")
		}
	}
}
