package clickhouse

import (
	"context"
	nurl "net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestOptionsFromURL(t *testing.T) {
	parse := func(t *testing.T, dsn string) (*clickhouse.Options, error) {
		t.Helper()
		u, err := nurl.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		return optionsFromURL(u)
	}

	t.Run("translates clickhouse-go v1 parameters", func(t *testing.T) {
		opts, err := parse(t, "clickhouse://host1:9000?username=user&password=pass&database=db"+
			"&read_timeout=10&timeout=2.5&write_timeout=5&block_size=100&no_delay=false&pool_size=10"+
			"&alt_hosts=host2:9000,host3:9000&connection_open_strategy=time_random"+
			"&x-multi-statement=true&x-migrations-table-engine=MergeTree")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := strings.Join(opts.Addr, ","), "host1:9000,host2:9000,host3:9000"; got != want {
			t.Errorf("Addr = %q, want %q", got, want)
		}
		if opts.ReadTimeout != 10*time.Second {
			t.Errorf("ReadTimeout = %v, want 10s", opts.ReadTimeout)
		}
		if opts.DialTimeout != 2500*time.Millisecond {
			t.Errorf("DialTimeout = %v, want 2.5s", opts.DialTimeout)
		}
		if opts.ConnOpenStrategy != clickhouse.ConnOpenRandom {
			t.Errorf("ConnOpenStrategy = %v, want random", opts.ConnOpenStrategy)
		}
		if opts.Auth.Username != "user" || opts.Auth.Password != "pass" || opts.Auth.Database != "db" {
			t.Errorf("Auth = %+v, want user/pass/db", opts.Auth)
		}
		// Anything left over would be sent to the server as a setting and rejected.
		if len(opts.Settings) != 0 {
			t.Errorf("Settings = %v, want none", opts.Settings)
		}
		if opts.GetJWT != nil {
			t.Error("GetJWT is set without a JWT parameter")
		}
	})

	t.Run("adds IPv6 alternate hosts after the primary", func(t *testing.T) {
		opts, err := parse(t, "clickhouse://[::1]:9000?alt_hosts=[::2]:9000,host3:9000")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := strings.Join(opts.Addr, ","), "[::1]:9000,[::2]:9000,host3:9000"; got != want {
			t.Errorf("Addr = %q, want %q", got, want)
		}
	})

	t.Run("keeps v1 zero timeouts disabled", func(t *testing.T) {
		// clickhouse-go v2 replaces a zero timeout with its default.
		opts, err := parse(t, "clickhouse://host:9000?read_timeout=0&timeout=0")
		if err != nil {
			t.Fatal(err)
		}
		if opts.ReadTimeout != noTimeout || opts.DialTimeout != noTimeout {
			t.Errorf("ReadTimeout = %v, DialTimeout = %v, want %v", opts.ReadTimeout, opts.DialTimeout, noTimeout)
		}
	})

	t.Run("sends a literal JWT", func(t *testing.T) {
		opts, err := parse(t, "clickhouse://host:9440?secure=true&database=db&x-jwt-token=token-a")
		if err != nil {
			t.Fatal(err)
		}
		if opts.GetJWT == nil {
			t.Fatal("GetJWT is not set")
		}
		token, err := opts.GetJWT(context.Background())
		if err != nil || token != "token-a" {
			t.Errorf("GetJWT() = %q, %v, want token-a", token, err)
		}
		if opts.Auth.Username != "" || opts.Auth.Password != "" {
			t.Errorf("Auth = %+v, want no username or password", opts.Auth)
		}
	})

	t.Run("re-reads the JWT file for every connection", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte("token-a\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		opts, err := parse(t, "clickhouse://host:9440?secure=true&x-jwt-token-file="+nurl.QueryEscape(path))
		if err != nil {
			t.Fatal(err)
		}
		if token, err := opts.GetJWT(context.Background()); err != nil || token != "token-a" {
			t.Errorf("GetJWT() = %q, %v, want token-a", token, err)
		}
		if err := os.WriteFile(path, []byte("token-b"), 0o600); err != nil {
			t.Fatal(err)
		}
		if token, err := opts.GetJWT(context.Background()); err != nil || token != "token-b" {
			t.Errorf("GetJWT() after rotation = %q, %v, want token-b", token, err)
		}
		if err := os.WriteFile(path, []byte("  \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := opts.GetJWT(context.Background()); err == nil {
			t.Error("GetJWT() with an empty file returned no error")
		}
	})

	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{"rejects tls_config", "clickhouse://host:9000?tls_config=custom", "tls_config"},
		{"rejects two JWT sources", "clickhouse://host:9440?x-jwt-token=a&x-jwt-token-file=/tmp/t", "x-jwt-token-file"},
		{"rejects a JWT with a username", "clickhouse://host:9440?username=user&x-jwt-token=a", "username"},
		{"rejects a JWT with credentials in the host", "clickhouse://user:pass@host:9440?x-jwt-token=a", "username"},
		{"rejects a JWT without TLS", "clickhouse://host:9000?x-jwt-token=a", "secure=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parse(t, tc.dsn)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want one mentioning %q", err, tc.want)
			}
		})
	}
}
