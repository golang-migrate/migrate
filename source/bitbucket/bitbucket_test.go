package bitbucket

import (
	"bytes"
	"context"
	"os"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

var BitbucketTestSecret = "" // username:password

func init() {
	secrets, err := os.ReadFile(".bitbucket_test_secrets")
	if err == nil {
		BitbucketTestSecret = string(bytes.TrimSpace(secrets)[:])
	}
}

func Test(t *testing.T) {
	if len(BitbucketTestSecret) == 0 {
		t.Skip("test requires .bitbucket_test_secrets")
	}

	b := &Bitbucket{}

	d, err := b.Open(context.Background(), "bitbucket://"+BitbucketTestSecret+"@abhishekbipp/test-migration/migrations/test#master")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
