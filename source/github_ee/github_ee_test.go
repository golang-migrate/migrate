package github_ee

import (
	"net/http"
	"net/http/httptest"
	nurl "net/url"
	"strings"
	"testing"
)

func Test(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.Join(strings.Split(r.URL.Path, "/")[:3], "/")

		if p != "/api/v3" {
			t.Fatal()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	}))
	defer ts.Close()

	u, err := nurl.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	g := &GithubEE{}
	_, err = g.Open("github-ee://foo:bar@" + u.Host + "/mattes/migrate_test_tmp/test?skipSSLVerify=true#452b8003e7")

	if err != nil {
		t.Fatal(err)
	}
}
