package migrate

import (
	"testing"
)

func TestSuintPanicsWithNegativeInput(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected suint to panic for -1")
		}
	}()
	suint(-1)
}

func TestSuint(t *testing.T) {
	if u := suint(0); u != 0 {
		t.Fatalf("expected 0, got %v", u)
	}
}
