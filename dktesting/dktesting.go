package dktesting

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/dhui/dktest"
	"github.com/moby/moby/client"
)

// notReadyErrText is part of the error dktest returns when a container is not ready before
// Options.Timeout. dktest has no sentinel error for this case, so the text is matched.
// TestNotReadyErrText fails if a dktest upgrade changes the text.
const notReadyErrText = "timed out waiting for container to get ready"

// maxContainerStarts is how many containers are started for a ContainerSpec with RetryNotReady
// before the test fails because none of them got ready.
const maxContainerStarts = 2

// runContext is dktest.RunContext. Tests replace it to check runWithRetry without Docker.
var runContext = dktest.RunContext

// ContainerSpec holds Docker testing setup specifications
type ContainerSpec struct {
	ImageName string
	Options   dktest.Options
	// RetryNotReady starts a new container if the first one does not get ready before
	// Options.Timeout. Use it only for images that are known to fail to start now and then.
	RetryNotReady bool
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
		spec := spec // capture range variable, see https://goo.gl/60w3p2

		// Only test against one version in short mode
		// TODO: order is random, maybe always pick first version instead?
		if i > 0 && testing.Short() {
			t.Logf("Skipping %v in short mode", spec.ImageName)
		} else {
			t.Run(spec.ImageName, func(t *testing.T) {
				t.Parallel()
				if err := runWithRetry(t, spec, testFunc); err != nil {
					t.Fatal("Failed:", err)
				}
			})
		}
	}
}

// runWithRetry runs testFunc in a container like dktest.Run does. If spec.RetryNotReady is set,
// it starts a new container when the last one did not get ready in time. The returned error
// holds the error of every container that was started.
func runWithRetry(t *testing.T, spec ContainerSpec, testFunc func(*testing.T, dktest.ContainerInfo)) error {
	starts := 1
	if spec.RetryNotReady {
		starts = maxContainerStarts
	}
	var errs []error
	for start := 1; start <= starts; start++ {
		err := runContext(context.Background(), t, spec.ImageName, spec.Options,
			func(c dktest.ContainerInfo) error {
				testFunc(t, c)
				return nil
			})
		if err == nil {
			return nil
		}
		errs = append(errs, err)
		if !isNotReady(err) {
			break
		}
		if start < starts {
			t.Log("Starting a new container because the last one did not get ready:", err)
		}
	}
	return errors.Join(errs...)
}

// isNotReady reports whether err means that a container did not get ready in time: either the
// ready check timed out, or the timeout was hit while the image was pulled or the container was
// created, started or inspected.
func isNotReady(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), notReadyErrText)
}
