package dktesting

import (
	"testing"
)

import (
	"github.com/dhui/dktest"
)

// ContainerSpec holds Docker testing setup specifications
type ContainerSpec struct {
	ImageName string
	Options   dktest.Options
}

// ParallelTest runs Docker tests in parallel
func ParallelTest(t *testing.T, specs []ContainerSpec,
	testFunc func(*testing.T, dktest.ContainerInfo)) {

	for i, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2

		// Only test against one version in short mode
		// TODO: order is random, maybe always pick first version instead?
		if i > 0 && testing.Short() {
			t.Logf("Skipping %v in short mode", spec.ImageName)
		} else {
			t.Run(spec.ImageName, func(t *testing.T) {
				t.Parallel()
				dktest.Run(t, spec.ImageName, spec.Options, testFunc)
			})
		}
	}
}
