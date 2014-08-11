package searchpath

import (
	"testing"
)

func TestSetSearchPath(t *testing.T) {
	SetSearchPath("a")
	if len(searchpath) != 1 || searchpath[0] != "a" {
		t.Error("SetSearchPath failed")
	}
}

func TestAppendSearchPath(t *testing.T) {
	SetSearchPath("a")
	AppendSearchPath("b")
	if len(searchpath) != 2 || searchpath[0] != "a" || searchpath[1] != "b" {
		t.Error("AppendSearchPath failed")
	}
}

func TestPrependSearchPath(t *testing.T) {
	SetSearchPath("a")
	PrependSearchPath("b")
	if len(searchpath) != 2 || searchpath[0] != "b" || searchpath[1] != "a" {
		t.Error("PrependSearchPath failed")
	}
}
