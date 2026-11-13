package dktesting

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const (
	defaultPullTimeout = time.Minute
	defaultTimeout     = time.Minute
	readyCheckTimeout  = 2 * time.Second
	readyPollInterval  = 250 * time.Millisecond
	cleanupTimeout     = 15 * time.Second
	// logTail is how many lines of a container's logs are shown when a test fails.
	logTail = "500"
	// maxContainerStarts is how many containers are started for a ContainerSpec
	// with RetryNotReady before the test fails.
	maxContainerStarts = 2

	// containerLabel marks containers started by migrate tests.
	// `make kill-orphaned-docker-containers` relies on it.
	containerLabel   = "migrate_test"
	dockerAPIVersion = "1.41"
)

// errNotReady means a container exited or did not get ready before
// Options.Timeout. Only this error is retried for RetryNotReady.
var errNotReady = errors.New("container not ready")

// Options configures how a test container is run.
type Options struct {
	// Env is the container environment.
	Env map[string]string
	// Cmd overrides the image's default command.
	Cmd []string
	// PortRequired fails the run if the container publishes no ports.
	PortRequired bool
	// ReadyFunc reports whether the container is ready for tests. It is polled
	// until it returns true or Timeout is reached, and must not block past the
	// Context it is given.
	ReadyFunc func(context.Context, ContainerInfo) bool
	// PullTimeout bounds the image pull. Defaults to one minute.
	PullTimeout time.Duration
	// Timeout bounds container start-up and the ready checks. Defaults to one minute.
	Timeout time.Duration
}

func (o Options) env() []string {
	env := make([]string, 0, len(o.Env))
	for k, v := range o.Env {
		env = append(env, k+"="+v)
	}
	slices.Sort(env)
	return env
}

// ContainerInfo describes a running test container.
type ContainerInfo struct {
	ID        string
	ImageName string
	Ports     network.PortMap
}

// Port returns the host address and port that the given container TCP port is published on.
func (c ContainerInfo) Port(containerPort uint16) (hostIP string, hostPort string, err error) {
	p, ok := network.PortFrom(containerPort, network.TCP)
	if !ok {
		return "", "", fmt.Errorf("invalid port %d", containerPort)
	}
	// Docker publishes on both 0.0.0.0 and ::. Callers build "host:port" strings,
	// so only IPv4 bindings are returned.
	for _, b := range c.Ports[p] {
		if !b.HostIP.IsValid() || b.HostIP == netip.IPv4Unspecified() {
			return "127.0.0.1", b.HostPort, nil
		}
		if b.HostIP.Is4() {
			return b.HostIP.String(), b.HostPort, nil
		}
	}
	return "", "", fmt.Errorf("no published IPv4 port for %v (IPv6-only bindings are not supported)", p)
}

// FirstPort returns the published host address and port for the lowest-numbered
// container TCP port. Use Port when the image exposes more than one port.
func (c ContainerInfo) FirstPort() (hostIP string, hostPort string, err error) {
	ports := slices.SortedFunc(maps.Keys(c.Ports), func(a, b network.Port) int {
		return int(a.Num()) - int(b.Num())
	})
	for _, p := range ports {
		if p.Proto() != network.TCP {
			continue
		}
		if hostIP, hostPort, err := c.Port(p.Num()); err == nil {
			return hostIP, hostPort, nil
		}
	}
	return "", "", errors.New("no published IPv4 port found (IPv6-only bindings are not supported)")
}

func newClient() (*client.Client, error) {
	return client.New(client.FromEnv, client.WithAPIVersion(dockerAPIVersion))
}

// run pulls the spec's image, starts a container from it, waits for
// Options.ReadyFunc, runs testFunc, and then removes the container.
// Any failure fails t.
func run(t *testing.T, spec ContainerSpec, testFunc func(*testing.T, ContainerInfo)) {
	t.Helper()
	opts := spec.Options
	if opts.PullTimeout <= 0 {
		opts.PullTimeout = defaultPullTimeout
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}

	dc, err := newClient()
	if err != nil {
		t.Fatal("creating Docker client:", err)
	}
	t.Cleanup(func() {
		if err := dc.Close(); err != nil {
			t.Error("closing Docker client:", err)
		}
	})

	if err := pullOnce(dc, spec.ImageName, opts.PullTimeout); err != nil {
		t.Fatalf("pulling %s: %v", spec.ImageName, err)
	}

	attempts := 1
	if spec.RetryNotReady {
		attempts = maxContainerStarts
	}
	c, err := startWithRetry(t, attempts, func() (ContainerInfo, error) {
		return startReady(t, dc, spec.ImageName, opts)
	})
	if err != nil {
		t.Fatalf("starting %s: %v", spec.ImageName, err)
	}

	testFunc(t, c)
}

// startWithRetry calls start up to attempts times, starting again only when the
// last container returned errNotReady. The returned error joins the error of
// every attempt.
func startWithRetry(t *testing.T, attempts int, start func() (ContainerInfo, error)) (ContainerInfo, error) {
	var errs []error
	for i := 1; i <= attempts; i++ {
		c, err := start()
		if err == nil {
			return c, nil
		}
		errs = append(errs, err)
		if !errors.Is(err, errNotReady) {
			break
		}
		if i < attempts {
			t.Log("starting a new container because the last one did not get ready:", err)
		}
	}
	return ContainerInfo{}, errors.Join(errs...)
}

// startReady starts a container and waits until it is ready. On success the
// container is removed when the test ends. On failure it is removed at once,
// after its logs are written to the test output.
func startReady(t *testing.T, dc *client.Client, imageName string, opts Options) (ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	c, err := startContainer(ctx, dc, imageName, opts)
	if err == nil {
		t.Logf("started container %.12s for %s, ports: %v", c.ID, imageName, c.Ports)
		err = waitReady(ctx, dc, c, opts.ReadyFunc)
	}
	if err != nil {
		if c.ID != "" {
			removeContainer(t, dc, c, true)
		}
		return ContainerInfo{}, err
	}
	t.Cleanup(func() { removeContainer(t, dc, c, t.Failed()) })
	return c, nil
}

// pulledImage records whether an image has been pulled. Its mutex makes
// parallel tests wait for a pull in progress instead of starting another.
type pulledImage struct {
	mu     sync.Mutex
	pulled bool
}

// pulls holds a *pulledImage per image name, so each image is pulled once per
// test binary even though many tests run the same images. Failed pulls are not
// remembered, so the next test tries again. ContainerSpec.Cleanup forgets the
// image when it removes it.
var pulls sync.Map

func pullOnce(dc *client.Client, imageName string, timeout time.Duration) error {
	v, _ := pulls.LoadOrStore(imageName, &pulledImage{})
	img := v.(*pulledImage)
	img.mu.Lock()
	defer img.mu.Unlock()
	if img.pulled {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := dc.ImagePull(ctx, imageName, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	if err := resp.Wait(ctx); err != nil {
		return err
	}
	img.pulled = true
	return nil
}

func startContainer(ctx context.Context, dc *client.Client, imageName string, opts Options) (ContainerInfo, error) {
	created, err := dc.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:  imageName,
			Env:    opts.env(),
			Cmd:    opts.Cmd,
			Labels: map[string]string{containerLabel: "true"},
		},
		HostConfig: &container.HostConfig{PublishAllPorts: true},
	})
	if err != nil {
		return ContainerInfo{}, fmt.Errorf("creating container: %w", err)
	}
	c := ContainerInfo{ID: created.ID, ImageName: imageName}

	if _, err := dc.ContainerStart(ctx, c.ID, client.ContainerStartOptions{}); err != nil {
		return c, fmt.Errorf("starting container: %w", err)
	}

	inspected, err := dc.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
	if err != nil {
		return c, fmt.Errorf("inspecting container: %w", err)
	}
	if ns := inspected.Container.NetworkSettings; ns != nil {
		c.Ports = ns.Ports
	}
	if opts.PortRequired && len(c.Ports) == 0 {
		return c, errors.New("container published no ports but PortRequired is set")
	}
	return c, nil
}

// waitReady polls ready until it returns true. It returns errNotReady if ctx
// runs out or the container stops running, so a crashed container fails fast
// instead of being polled until the timeout.
func waitReady(ctx context.Context, dc *client.Client, c ContainerInfo, ready func(context.Context, ContainerInfo) bool) error {
	if ready == nil {
		return nil
	}
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()
	for {
		checkCtx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
		ok := ready(checkCtx, c)
		cancel()
		if ok {
			return nil
		}
		if inspected, err := dc.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{}); err == nil {
			if s := inspected.Container.State; s != nil && !s.Running {
				return fmt.Errorf("%w: container is %s with exit code %d", errNotReady, s.Status, s.ExitCode)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errNotReady, ctx.Err())
		case <-ticker.C:
		}
	}
}

// removeContainer force-removes the container, first writing its logs to the
// test output if withLogs is set.
func removeContainer(t *testing.T, dc *client.Client, c ContainerInfo, withLogs bool) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	if withLogs {
		logContainer(ctx, t, dc, c)
	}
	_, err := dc.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	if err != nil && !errdefs.IsNotFound(err) {
		t.Errorf("removing container %.12s: %v", c.ID, err)
	}
}

func logContainer(ctx context.Context, t *testing.T, dc *client.Client, c ContainerInfo) {
	logs, err := dc.ContainerLogs(ctx, c.ID, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: logTail})
	if err != nil {
		t.Logf("reading logs of container %.12s: %v", c.ID, err)
		return
	}
	defer func() {
		if err := logs.Close(); err != nil {
			t.Logf("closing logs of container %.12s: %v", c.ID, err)
		}
	}()
	var out strings.Builder
	if _, err := stdcopy.StdCopy(&out, &out, logs); err != nil {
		t.Logf("reading logs of container %.12s: %v", c.ID, err)
	}
	t.Logf("last %s log lines of container %.12s (%s):\n%s", logTail, c.ID, c.ImageName, out.String())
}
