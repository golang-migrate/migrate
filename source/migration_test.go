package source

import (
	"testing"
)

func TestNewMigrations(t *testing.T) {
	m := NewMigrations()

	if m == nil {
		t.Fatal("expected non-nil Migrations")
	}

	if len(m.index) != 0 {
		t.Fatalf("expected empty index, got %d", len(m.index))
	}

	if len(m.migrations) != 0 {
		t.Fatalf("expected empty migrations map, got %d", len(m.migrations))
	}
}

func TestAppend(t *testing.T) {
	m := NewMigrations()

	if m.Append(nil) {
		t.Fatal("Append(nil) should return false")
	}

	up := &Migration{
		Version:    1,
		Identifier: "create_users",
		Direction:  Up,
		Raw:        "1_create_users.up.sql",
	}

	if !m.Append(up) {
		t.Fatal("expected first append to succeed")
	}

	if len(m.index) != 1 {
		t.Fatalf("expected index length 1, got %d", len(m.index))
	}

	dup := &Migration{
		Version:    1,
		Identifier: "duplicate",
		Direction:  Up,
		Raw:        "1_duplicate.up.sql",
	}

	if m.Append(dup) {
		t.Fatal("expected duplicate migration to be rejected")
	}

	down := &Migration{
		Version:    1,
		Identifier: "create_users",
		Direction:  Down,
		Raw:        "1_create_users.down.sql",
	}

	if !m.Append(down) {
		t.Fatal("expected down migration to succeed")
	}

	if len(m.index) != 1 {
		t.Fatalf("expected only one version in index, got %d", len(m.index))
	}
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
	m := NewMigrations()

	m.Append(&Migration{Version: 3, Direction: Up})
	m.Append(&Migration{Version: 1, Direction: Up})
	m.Append(&Migration{Version: 2, Direction: Up})

	expected := []uint{1, 2, 3}

	if len(m.index) != len(expected) {
		t.Fatalf("expected %d versions, got %d", len(expected), len(m.index))
	}

	for i, v := range expected {
		if m.index[i] != v {
			t.Fatalf("expected index[%d]=%d, got %d", i, v, m.index[i])
		}
	}
}

func TestFirst(t *testing.T) {
	m := NewMigrations()

	if _, ok := m.First(); ok {
		t.Fatal("expected First() on empty migrations to fail")
	}

	m.Append(&Migration{Version: 5, Direction: Up})
	m.Append(&Migration{Version: 2, Direction: Up})
	m.Append(&Migration{Version: 9, Direction: Up})

	v, ok := m.First()

	if !ok {
		t.Fatal("expected First() to succeed")
	}

	if v != 2 {
		t.Fatalf("expected first version 2, got %d", v)
	}
}

func TestPrev(t *testing.T) {
	m := NewMigrations()

	m.Append(&Migration{Version: 1, Direction: Up})
	m.Append(&Migration{Version: 3, Direction: Up})
	m.Append(&Migration{Version: 5, Direction: Up})

	tests := []struct {
		version uint
		ok      bool
		prev    uint
	}{
		{1, false, 0},
		{2, false, 0},
		{3, true, 1},
		{5, true, 3},
		{9, false, 0},
	}

	for _, tc := range tests {
		prev, ok := m.Prev(tc.version)

		if ok != tc.ok {
			t.Errorf("Prev(%d): expected ok=%v, got %v", tc.version, tc.ok, ok)
			continue
		}

		if ok && prev != tc.prev {
			t.Errorf("Prev(%d): expected %d, got %d", tc.version, tc.prev, prev)
		}
	}
}

func TestUp(t *testing.T) {
	m := NewMigrations()

	up := &Migration{
		Version:   1,
		Direction: Up,
	}

	m.Append(up)

	got, ok := m.Up(1)

	if !ok {
		t.Fatal("expected Up(1) to succeed")
	}

	if got != up {
		t.Fatal("returned migration does not match")
	}

	m.Append(&Migration{
		Version:   2,
		Direction: Down,
	})

	if _, ok := m.Up(2); ok {
		t.Fatal("expected Up(2) to fail")
	}

	if _, ok := m.Up(99); ok {
		t.Fatal("expected Up(99) to fail")
	}
}

func TestDown(t *testing.T) {
	m := NewMigrations()

	down := &Migration{
		Version:   1,
		Direction: Down,
	}

	m.Append(down)

	got, ok := m.Down(1)

	if !ok {
		t.Fatal("expected Down(1) to succeed")
	}

	if got != down {
		t.Fatal("returned migration does not match")
	}

	m.Append(&Migration{
		Version:   2,
		Direction: Up,
	})

	if _, ok := m.Down(2); ok {
		t.Fatal("expected Down(2) to fail")
	}

	if _, ok := m.Down(99); ok {
		t.Fatal("expected Down(99) to fail")
	}
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
