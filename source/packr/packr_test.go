package packr

import (
	"github.com/gobuffalo/packr"
	"log"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func Test(t *testing.T) {
	box := packr.NewBox("./testdata")

	d, err := WithInstance(&box)
	if err != nil {
		log.Panicf("Error during create migrator driver: %v", err)
	}

	st.Test(t, d)
}

func TestOpen(t *testing.T) {
	p := &Packr{}
	_, err := p.Open("")
	if err == nil {
		t.Fatal("expected err, because it's not implemented yet")
	}
}
