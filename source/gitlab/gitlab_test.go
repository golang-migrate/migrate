package gitlab

import (
	"bytes"
	"io/ioutil"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

var GitlabTestSecret = "" // username:token

func init() {
	secrets, err := ioutil.ReadFile(".gitlab_test_secrets")
	if err == nil {
		GitlabTestSecret = string(bytes.TrimSpace(secrets)[:])
	}
}

func Test(t *testing.T) {
	if len(GitlabTestSecret) == 0 {
		t.Skip("test requires .gitlab_test_secrets")
	}

	g := &Gitlab{}
	d, err := g.Open("gitlab://" + GitlabTestSecret + "@gitlab.com/11197284/migrations")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
