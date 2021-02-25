package mem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrDuplicateVersion_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := ErrDuplicateVersion{}
	assert.Equal(t, ErrDuplicateVersion{}.Error(), err.Error())
}
