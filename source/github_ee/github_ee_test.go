package github_ee

import (
	"net/http"
	"net/http/httptest"
	nurl "net/url"
	"testing"
)

func Test(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/repos/mattes/migrate_test_tmp/contents/test" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if ref := r.URL.Query().Get("ref"); ref != "452b8003e7" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_, err := w.Write([]byte("[]"))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer ts.Close()

	u, err := nurl.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	g := &GithubEE{}
	_, err = g.Open("github-ee://foo:bar@" + u.Host + "/mattes/migrate_test_tmp/test?verify-tls=false#452b8003e7")

	if err != nil {
		t.Fatal(err)
	}
}
