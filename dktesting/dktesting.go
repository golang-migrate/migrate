// Package dktesting runs migrate's database driver tests against Docker containers.
package dktesting

import (
	"context"
	"fmt"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// ContainerSpec holds Docker testing setup specifications
type ContainerSpec struct {
	ImageName string
	Options   Options
	// RetryNotReady starts a new container if the first one does not get ready
	// before Options.Timeout. Use it only for images that are known to fail to
	// start now and then.
	RetryNotReady bool
}

// Cleanup cleanups the ContainerSpec after a test run by removing the ContainerSpec's image
func (s *ContainerSpec) Cleanup() (retErr error) {
	dc, err := newClient()
	if err != nil {
		return err
	}
	defer func() {
		if err := dc.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("error closing Docker client: %w", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	// Forget the pull before removing, so a later test pulls the image again.
	pulls.Delete(s.ImageName)
	_, err = dc.ImageRemove(ctx, s.ImageName, client.ImageRemoveOptions{Force: true, PruneChildren: true})
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// ParallelTest runs Docker tests in parallel
func ParallelTest(t *testing.T, specs []ContainerSpec,
	testFunc func(*testing.T, ContainerInfo)) {

	for i, spec := range specs {
		// Only test against one version in short mode
		// TODO: order is random, maybe always pick first version instead?
		if i > 0 && testing.Short() {
			t.Logf("Skipping %v in short mode", spec.ImageName)
		} else {
			t.Run(spec.ImageName, func(t *testing.T) {
				t.Parallel()
				run(t, spec, testFunc)
			})
		}
	}
}
