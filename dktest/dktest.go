package dktest

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

var (
	// DefaultPullTimeout is the default timeout used when pulling images.
	DefaultPullTimeout = time.Minute
	// DefaultTimeout is the default timeout used when starting a container and checking if it's ready.
	DefaultTimeout = time.Minute
	// DefaultReadyTimeout is the default timeout used for each container ready check.
	DefaultReadyTimeout = 2 * time.Second
	// DefaultCleanupTimeout is the default timeout used when stopping and removing a container.
	DefaultCleanupTimeout = 15 * time.Second
)

const label = "dktest"

// Run runs the given test function once the specified Docker image is running.
func Run(t *testing.T, imgName string, opts Options, testFunc func(*testing.T, ContainerInfo)) {
	err := RunContext(context.Background(), t, imgName, opts, func(containerInfo ContainerInfo) error {
		testFunc(t, containerInfo)
		return nil
	})
	if err != nil {
		t.Fatal("Failed:", err)
	}
}

// RunContext is similar to Run, but takes a parent context and returns errors.
func RunContext(ctx context.Context, logger Logger, imgName string, opts Options, testFunc func(ContainerInfo) error) (retErr error) {
	dc, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("error getting Docker client: %w", err)
	}
	defer func() {
		if err := dc.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("error closing Docker client: %w", err)
		}
	}()

	opts.init()
	pullCtx, pullCancel := context.WithTimeout(ctx, opts.PullTimeout)
	defer pullCancel()

	if err := pullImage(pullCtx, logger, dc, opts.PullRegistryAuth, imgName, opts.Platform); err != nil {
		return fmt.Errorf("error pulling image %s: %w", imgName, err)
	}

	runCtx, runCancel := context.WithTimeout(ctx, opts.Timeout)
	defer runCancel()

	c, err := runImage(runCtx, logger, dc, imgName, opts)
	if err != nil {
		return fmt.Errorf("error running image %s: %w", imgName, err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(ctx, opts.CleanupTimeout)
		defer stopCancel()
		stopContainer(stopCtx, logger, dc, c, opts.LogStdout, opts.LogStderr)
		if opts.CleanupImage {
			removeImage(stopCtx, logger, dc, imgName)
		}
	}()

	if !waitContainerReady(runCtx, logger, c, opts.ReadyFunc, opts.ReadyTimeout) {
		return fmt.Errorf("timed out waiting for container to get ready: %v", c.String())
	}
	if err := testFunc(c); err != nil {
		return fmt.Errorf("error running test func: %w", err)
	}
	return nil
}

func pullImage(ctx context.Context, logger Logger, dc *client.Client, registryAuth, imgName, platform string) error {
	logger.Log("Pulling image:", imgName)

	opts := client.ImagePullOptions{RegistryAuth: registryAuth}
	if platform != "" {
		if parsed, ok := parsePlatform(platform); ok {
			opts.Platforms = append(opts.Platforms, parsed)
		}
	}

	resp, err := dc.ImagePull(ctx, imgName, opts)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Close(); err != nil {
			logger.Log("Failed to close image response:", err)
		}
	}()

	var output strings.Builder
	if _, err := io.Copy(&output, resp); err != nil {
		return err
	}
	if output.Len() > 0 {
		logger.Log("Image pull response:", output.String())
	}
	return nil
}

func removeImage(ctx context.Context, logger Logger, dc *client.Client, imgName string) {
	logger.Log("Removing image:", imgName)
	if _, err := dc.ImageRemove(ctx, imgName, client.ImageRemoveOptions{Force: true, PruneChildren: true}); err != nil {
		logger.Log("Failed to remove image:", err)
	}
}

func runImage(ctx context.Context, logger Logger, dc *client.Client, imgName string, opts Options) (ContainerInfo, error) {
	c := ContainerInfo{Name: genContainerName(), ImageName: imgName}
	portBindings, err := toNetworkPortMap(opts.PortBindings)
	if err != nil {
		return c, err
	}
	exposedPorts, err := toNetworkPortSet(opts.ExposedPorts)
	if err != nil {
		return c, err
	}

	resp, err := dc.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:        imgName,
			Labels:       map[string]string{label: "true"},
			Env:          opts.env(),
			Entrypoint:   opts.Entrypoint,
			Cmd:          opts.Cmd,
			Volumes:      opts.volumes(),
			Hostname:     opts.Hostname,
			ExposedPorts: exposedPorts,
		},
		HostConfig: &container.HostConfig{
			PublishAllPorts: true,
			PortBindings:    portBindings,
			ShmSize:         opts.ShmSize,
			Mounts:          opts.Mounts,
		},
		NetworkingConfig: &network.NetworkingConfig{},
		Name:             c.Name,
	})
	if err != nil {
		return c, err
	}
	c.ID = resp.ID
	logger.Log("Created container:", c.String())

	if _, err := dc.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{}); err != nil {
		return c, err
	}
	logger.Log("Started container:", c.String())

	if !opts.PortRequired {
		return c, nil
	}

	inspectResp, err := dc.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
	if err != nil {
		return c, err
	}
	if inspectResp.Container.NetworkSettings == nil {
		return c, errNoNetworkSettings
	}
	c.Ports = fromNetworkPortMap(inspectResp.Container.NetworkSettings.Ports)
	logger.Log("Inspected container:", c.String())
	return c, nil
}

func stopContainer(ctx context.Context, logger Logger, dc *client.Client, c ContainerInfo, logStdout, logStderr bool) {
	if logStdout || logStderr {
		logs, err := dc.ContainerLogs(ctx, c.ID, client.ContainerLogsOptions{
			Timestamps: true,
			ShowStdout: logStdout,
			ShowStderr: logStderr,
		})
		if err != nil {
			logger.Log("Error fetching container logs:", err)
		} else {
			if b, err := io.ReadAll(logs); err == nil {
				logger.Log("Container logs:", string(b))
			} else {
				logger.Log("Error reading container logs:", err)
			}
			if err := logs.Close(); err != nil {
				logger.Log("Error closing logs:", err)
			}
		}
	}

	if _, err := dc.ContainerStop(ctx, c.ID, client.ContainerStopOptions{}); err != nil {
		logger.Log("Error stopping container:", c.String(), "error:", err)
	}
	if _, err := dc.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{RemoveVolumes: true, Force: true}); err != nil {
		logger.Log("Error removing container:", c.String(), "error:", err)
	}
	logger.Log("Removed container:", c.String())
}

func waitContainerReady(ctx context.Context, logger Logger, c ContainerInfo, readyFunc func(context.Context, ContainerInfo) bool, readyTimeout time.Duration) bool {
	if readyFunc == nil {
		return true
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
			ready := readyFunc(readyCtx, c)
			cancel()
			if ready {
				return true
			}
		case <-ctx.Done():
			logger.Log("Container was never ready:", c.String())
			return false
		}
	}
}
