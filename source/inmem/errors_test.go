package inmem

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestErrDuplicateVersion_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := ErrDuplicateVersion{}
	assert.Equal(t, ErrDuplicateVersion{}.Error(), err.Error())
}
