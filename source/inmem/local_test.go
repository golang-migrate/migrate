package inmem

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestRegisterMigrations_ErrEmptyKey(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := RegisterMigrations("")
	assert.Equal(t, ErrEmptyKey, err)
}

func TestRegisterMigrations_EmptyMap(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := RegisterMigrations(testKey)
	assert.Equal(t, nil, err)
}

func TestRegisterMigrations_ErrNilMigration(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := RegisterMigrations(testKey, nil)
	assert.Equal(t, ErrNilMigration, err)
}

func TestRegisterMigrations_ErrDuplicateVersion(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := RegisterMigrations(testKey, firstMigration, firstMigration)
	assert.NotEqual(t, nil, err)
}

func TestRegisterMigration(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	err := RegisterMigrations(testKey, firstMigration, secondMigration)
	assert.Equal(t, nil, err)
}
