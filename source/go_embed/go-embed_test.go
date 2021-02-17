package goembed

import (
	"testing"

	"github.com/golang-migrate/migrate/v4/source/go_embed/testdata"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func Test(t *testing.T) {
	d, err := WithEmbed(".", testdata.Source)
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

