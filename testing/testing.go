package testing

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

type IsReadyFunc func(Instance) bool

type TestFunc func(*testing.T, Instance)

type Version struct {
	Image string
	ENV   []string
}

func ParallelTest(t *testing.T, versions []Version, readyFn IsReadyFunc, testFn TestFunc) {
	delay, err := strconv.Atoi(os.Getenv("MIGRATE_TEST_CONTAINER_BOOT_DELAY"))
	if err != nil {
		delay = 0
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
				container, err := NewDockerContainer(t, version.Image, version.ENV)
				if err != nil {
					t.Fatalf("%v\n%s", err, containerLogs(t, container))
				}

				// make sure to remove container once done
				defer container.Remove()

				// wait until database is ready
				tick := time.Tick(1000 * time.Millisecond)
				timeout := time.After(time.Duration(delay+60) * time.Second)
			outer:
				for {
					select {
					case <-tick:
						if readyFn(container) {
							break outer
						}

					case <-timeout:
						t.Fatalf("Docker: Container not ready, timeout for %v.\n%s", version, containerLogs(t, container))
					}
				}

				time.Sleep(time.Duration(int64(delay)) * time.Second)

				// we can now run the tests
				testFn(t, container)
			})
		}
	}
}

func containerLogs(t *testing.T, c *DockerContainer) []byte {
	r, err := c.Logs()
	if err != nil {
		t.Error("%v", err)
		return nil
	}
	defer r.Close()
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error("%v", err)
		return nil
	}
	return b
}

type Instance interface {
	Host() string
	Port() uint
	KeepForDebugging()
}
