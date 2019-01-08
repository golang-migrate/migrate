package dktesting_test

import (
	"testing"
)

import (
	"github.com/dhui/dktest"
)

import (
	"github.com/golang-migrate/migrate/v4/dktesting"
)

func ExampleParallelTest() {
	t := &testing.T{} // Should actually be used in a Test

	var isReady = func(c dktest.ContainerInfo) bool {
		// Return true if the container is ready to run tests.
		// Don't block here though.
		return true
	}

	dktesting.ParallelTest(t, []dktesting.ContainerSpec{{ImageName: "docker_image:9.6",
		Options: dktest.Options{ReadyFunc: isReady}}}, func(t *testing.T, c dktest.ContainerInfo) {
		// Run your test/s ...
		t.Fatal("...")
	})
}
