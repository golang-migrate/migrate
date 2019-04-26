package migrate

import (
	"errors"
	nurl "net/url"
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

func TestFilterCustomQuery(t *testing.T) {
	n, err := nurl.Parse("foo://host?a=b&x-custom=foo&c=d")
	if err != nil {
		t.Fatal(err)
	}
	nx := FilterCustomQuery(n).Query()
	if nx.Get("x-custom") != "" {
		t.Fatalf("didn't expect x-custom")
	}
}

func TestSourceSchemeFromUrlSuccess(t *testing.T) {
	urlStr := "protocol://path"
	expected := "protocol"

	u, err := sourceSchemeFromURL(urlStr)
	if err != nil {
		t.Fatalf("expected no error, but received %q", err)
	}
	if u != expected {
		t.Fatalf("expected %q, but received %q", expected, u)
	}
}

func TestSourceSchemeFromUrlFailure(t *testing.T) {
	cases := []struct {
		name      string
		urlStr    string
		expectErr error
	}{
		{
			name:      "Empty",
			urlStr:    "",
			expectErr: errors.New("source: URL cannot be empty"),
		},
		{
			name:      "NoScheme",
			urlStr:    "hello",
			expectErr: errors.New("source: no scheme"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sourceSchemeFromURL(tc.urlStr)
			if err.Error() != tc.expectErr.Error() {
				t.Fatalf("expected %q, but received %q", tc.expectErr, err)
			}
		})
	}
}

func TestDatabaseSchemeFromUrlSuccess(t *testing.T) {
	urlStr := "protocol://path"
	expected := "protocol"

	u, err := databaseSchemeFromURL(urlStr)
	if err != nil {
		t.Fatalf("expected no error, but received %q", err)
	}
	if u != expected {
		t.Fatalf("expected %q, but received %q", expected, u)
	}
}

func TestDatabaseSchemeFromUrlFailure(t *testing.T) {
	cases := []struct {
		name      string
		urlStr    string
		expectErr error
	}{
		{
			name:      "Empty",
			urlStr:    "",
			expectErr: errors.New("database: URL cannot be empty"),
		},
		{
			name:      "NoScheme",
			urlStr:    "hello",
			expectErr: errors.New("database: no scheme"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := databaseSchemeFromURL(tc.urlStr)
			if err.Error() != tc.expectErr.Error() {
				t.Fatalf("expected %q, but received %q", tc.expectErr, err)
			}
		})
	}
}
