package packr

import (
	"os"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
	"github.com/stretchr/testify/assert"
)

func TestOK(t *testing.T) {
	var cases = []string{
		"packr://testdata/migrations",
		"packr://testdata/migrations/",
	}

	for _, tt := range cases {
		t.Run(tt, func(t *testing.T) {
			px := Packr{}
			d, err := px.Open(tt)
			assert.Nil(t, err)

			st.Test(t, d)
		})
	}
}

func TestDoesNotExist(t *testing.T) {
	px := Packr{}
	d, err := px.Open("packr://testdata/xxx")
	assert.Nil(t, err)

	_, err = d.First()
	assert.IsType(t, &os.PathError{}, err)
}

func TestEmpty(t *testing.T) {
	px := Packr{}
	d, err := px.Open("packr://testdata")
	assert.Nil(t, err)

	r, _, err := d.ReadUp(1)
	assert.Nil(t, r)
}
