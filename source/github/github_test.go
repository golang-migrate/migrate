package github

import (
	"bytes"
	"io/ioutil"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

var GitHubTestSecret = "" // username:token

func init() {
	secrets, err := ioutil.ReadFile(".github_test_secrets")
	if err == nil {
		GitHubTestSecret = string(bytes.TrimSpace(secrets)[:])
	}
}

func Test(t *testing.T) {
	if len(GitHubTestSecret) == 0 {
		t.Skip("test requires .github_test_secrets")
	}

	g := &GitHub{}
	d, err := g.Open("github://" + GitHubTestSecret + "@mattes/migrate_test_tmp/test#452b8003e7")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
