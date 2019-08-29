package url

import (
	"testing"
)

func TestSchemeFromUrl(t *testing.T) {
	cases := []struct {
		name      string
		urlStr    string
		expected  string
		expectErr error
	}{
		{
			name:     "Simple",
			urlStr:   "protocol://path",
			expected: "protocol",
		},
		{
			// See issue #264
			name:     "MySQLWithPort",
			urlStr:   "mysql://user:pass@tcp(host:1337)/db",
			expected: "mysql",
		},
		{
			name:      "Empty",
			urlStr:    "",
			expectErr: errEmptyURL,
		},
		{
			name:      "NoScheme",
			urlStr:    "hello",
			expectErr: errNoScheme,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := SchemeFromURL(tc.urlStr)
			if err != tc.expectErr {
				t.Fatalf("expected %q, but received %q", tc.expectErr, err)
			}
			if s != tc.expected {
				t.Fatalf("expected %q, but received %q", tc.expected, s)
			}
		})
	}
}
