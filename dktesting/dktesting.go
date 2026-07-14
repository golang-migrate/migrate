package dktesting

import (
	"context"
	"fmt"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/dhui/dktest"
	"github.com/moby/moby/client"
)

// ContainerSpec holds Docker testing setup specifications
type ContainerSpec struct {
	ImageName string
	Options   dktest.Options
}

// Cleanup cleanups the ContainerSpec after a test run by removing the ContainerSpec's image
func (s *ContainerSpec) Cleanup() (retErr error) {
	// copied from dktest.RunContext()
	dc, err := client.New(client.FromEnv, client.WithAPIVersion("1.41"))
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
	if _, err := dc.ImageRemove(ctx, s.ImageName, client.ImageRemoveOptions{Force: true, PruneChildren: true}); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// ParallelTest runs Docker tests in parallel
func ParallelTest(t *testing.T, specs []ContainerSpec,
	testFunc func(*testing.T, dktest.ContainerInfo)) {

	for i, spec := range specs {

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
