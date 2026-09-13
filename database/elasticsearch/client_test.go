package elasticsearch

import (
	"testing"
)

func TestIsUnder(t *testing.T) {
	t.Run("isUnder happy path", func(t *testing.T) {
		result := isUnder("/foo/bar", "/foo")
		if !result {
			t.Errorf("expected isUnder(\"/foo/bar\", \"/foo\") to be true, got false")
		}
	})

	t.Run("isUnder not under", func(t *testing.T) {
		result := isUnder("/foo/bar", "/baz")
		if result {
			t.Errorf("expected isUnder(\"/foo/bar\", \"/baz\") to be false, got true")
		}
	})

	t.Run("isUnder same path", func(t *testing.T) {
		result := isUnder("/foo/bar", "/foo/bar")
		if !result {
			t.Errorf("expected isUnder(\"/foo/bar\", \"/foo/bar\") to be true, got false")
		}
	})

	t.Run("isUnder with trailing slash", func(t *testing.T) {
		result := isUnder("/foo/bar", "/foo/")
		if !result {
			t.Errorf("expected isUnder(\"/foo/bar\", \"/foo/\") to be true, got false")
		}
	})

	t.Run("isUnder with empty base", func(t *testing.T) {
		result := isUnder("/foo/bar", "")
		if !result {
			t.Errorf("expected isUnder(\"/foo/bar\", \"\") to be true, got false")
		}
	})

	t.Run("isUnder, but with ../ in path", func(t *testing.T) {
		result := isUnder("/foo/bar/../baz", "/foo")
		if !result {
			t.Errorf("expected isUnder(\"/foo/bar/../baz\", \"/foo\") to be true, got false")
		}
	})
}
