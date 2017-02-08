package testing

import (
	"os"
	"strconv"
	"testing"
	"time"
)

type IsReadyFunc func(Instance) bool

type TestFunc func(*testing.T, Instance)

func ParallelTest(t *testing.T, versions []string, readyFn IsReadyFunc, testFn TestFunc) {
	for i, version := range versions {
		version := version // capture range variable, see https://goo.gl/60w3p2

		// Only test against first found version in short mode
		if i > 0 && testing.Short() {
			t.Logf("Skipping %v in short mode", version)

		} else {
			t.Run(version, func(t *testing.T) {
				t.Parallel()

				// creata new container
				container, err := NewDockerContainer(t, version)
				if err != nil {
					t.Fatal(err)
				}

				// make sure to remove container once done
				defer container.Remove()

				// wait until database is ready
				tick := time.Tick(1000 * time.Millisecond)
				timeout := time.After(30 * time.Second)
			outer:
				for {
					select {
					case <-tick:
						if readyFn(container) {
							break outer
						}

					case <-timeout:
						t.Fatalf("Docker: Container not ready, timeout for %v.", version)
					}
				}

				delay, err := strconv.Atoi(os.Getenv("MIGRATE_TEST_CONTAINER_BOOT_DELAY"))
				if err == nil {
					time.Sleep(time.Duration(int64(delay)) * time.Second)
				} else {
					time.Sleep(2 * time.Second)
				}

				// we can now run the tests
				testFn(t, container)
			})
		}
	}
}

type Instance interface {
	Host() string
	Port() uint
	KeepForDebugging()
}
