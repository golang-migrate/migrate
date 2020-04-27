package gitlab

import (
	"bytes"
	"io/ioutil"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

var GitlabTestUrl = "" // gitlab://username:token@gitlab.com/11197284/migrations?per_page=3

func init() {
	secrets, err := ioutil.ReadFile(".gitlab_test_url")
	if err == nil {
		GitlabTestUrl = string(bytes.TrimSpace(secrets)[:])
	}
}

func Test(t *testing.T) {
	if len(GitlabTestUrl) == 0 {
		t.Skip("test requires .gitlab_test_url")
	}

	g := &Gitlab{}
	d, err := g.Open(GitlabTestUrl)
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
