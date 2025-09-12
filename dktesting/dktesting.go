package dktesting

import (
	"context"
	"fmt"
	"testing"

	"github.com/dhui/dktest"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// ContainerSpec holds Docker testing setup specifications
type ContainerSpec struct {
	ImageName string
	Options   dktest.Options
}

// Cleanup cleanups the ContainerSpec after a test run by removing the ContainerSpec's image
func (s *ContainerSpec) Cleanup() (retErr error) {
	// copied from dktest.RunContext()
	dc, err := client.NewClientWithOpts(client.FromEnv, client.WithVersion("1.41"))
	if err != nil {
		return err
	}
	defer func() {
		if err := dc.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("error closing Docker client: %w", err)
		}
	}()
	cleanupTimeout := s.Options.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = dktest.DefaultCleanupTimeout
	}
	ctx, timeoutCancelFunc := context.WithTimeout(context.Background(), cleanupTimeout)
	defer timeoutCancelFunc()
	if _, err := dc.ImageRemove(ctx, s.ImageName, image.RemoveOptions{Force: true, PruneChildren: true}); err != nil {
		return err
	}
	return nil
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
