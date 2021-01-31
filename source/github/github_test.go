package github

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
	"github.com/stretchr/testify/assert"
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

	g := &Github{}
	d, err := g.Open("github://" + GithubTestSecret + "@mattes/migrate_test_tmp/test#452b8003e7")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

func TestDefaultClient(t *testing.T) {
	g := &Github{}
	owner := "golang-migrate"
	repo := "migrate"
	path := "source/github/examples/migrations"

	url := fmt.Sprintf("github://%s/%s/%s", owner, repo, path)
	d, err := g.Open(url)
	if err != nil {
		t.Fatal(err)
	}

	ver, err := d.First()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint(1085649617), ver)

	ver, err = d.Next(ver)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint(1185749658), ver)
}
