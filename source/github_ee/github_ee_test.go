package github_ee

import (
	"bytes"
	"io/ioutil"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

var GithubTestSecret = "" // username:token

func init() {
	secrets, err := ioutil.ReadFile(".github_test_secrets")
	if err == nil {
		GithubTestSecret = string(bytes.TrimSpace(secrets)[:])
	}
}

func Test(t *testing.T) {
	if len(GithubTestSecret) == 0 {
		t.Skip("test requires .github_test_secrets")
	}

	g := &GithubEE{}
	d, err := g.Open("github-ee://" + GithubTestSecret + "@github.com/mattes/migrate_test_tmp/test#452b8003e7")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
