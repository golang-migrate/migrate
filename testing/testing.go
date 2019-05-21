package testing

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	dockertypes "github.com/docker/docker/api/types"
)

type IsReadyFunc func(Instance) bool

type TestFunc func(*testing.T, Instance)

type Version struct {
	Image string
	ENV   []string
	Cmd   []string
}

func ParallelTest(t *testing.T, versions []Version, readyFn IsReadyFunc, testFn TestFunc) {
	timeout, err := strconv.Atoi(os.Getenv("MIGRATE_TEST_CONTAINER_BOOT_TIMEOUT"))
	if err != nil {
		timeout = 60 // Cassandra docker image can take ~30s to start
	}

	for i, version := range versions {
		version := version // capture range variable, see https://goo.gl/60w3p2

		// Only test against one version in short mode
		// TODO: order is random, maybe always pick first version instead?
		if i > 0 && testing.Short() {
			t.Logf("Skipping %v in short mode", version)

		} else {
			t.Run(version.Image, func(t *testing.T) {
				t.Parallel()

				// create new container
				container, err := NewDockerContainer(t, version.Image, version.ENV, version.Cmd)
				if err != nil {
					t.Fatalf("%v\n%s", err, containerLogs(t, container))
				}

				// make sure to remove container once done
				defer func() {
					if err := container.Remove(); err != nil {
						t.Error(err)
					}
				}()
				// wait until database is ready
				tick := time.NewTicker(1000 * time.Millisecond)
				defer tick.Stop()
				timeout := time.NewTimer(time.Duration(timeout) * time.Second)
				defer timeout.Stop()
			outer:
				for {
					select {
					case <-tick.C:
						if readyFn(container) {
							break outer
						}

					case <-timeout.C:
						t.Fatalf("Docker: Container not ready, timeout for %v.\n%s", version, containerLogs(t, container))
					}
				}

				// we can now run the tests
				testFn(t, container)
			})
		}
	}
}

func containerLogs(t *testing.T, c *DockerContainer) []byte {
	r, err := c.Logs()
	if err != nil {
		t.Error(err)
		return nil
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err)
		return nil
	}
	return b
}

type Instance interface {
	Host() string
	Port() uint
	PortFor(int) uint
	NetworkSettings() dockertypes.NetworkSettings
	KeepForDebugging()
}
