package dktest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

var (
	DefaultPullTimeout    = time.Minute
	DefaultTimeout        = time.Minute
	DefaultReadyTimeout   = 2 * time.Second
	DefaultCleanupTimeout = 15 * time.Second
)

const label = "dktest"

type pullMessage struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

func pullImage(ctx context.Context, lgr Logger, dc client.ImageAPIClient, registryAuth, imgName, platform string) error {
	lgr.Log("Pulling image:", imgName)

	resp, err := dc.ImagePull(ctx, imgName, image.PullOptions{
		Platform:     platform,
		RegistryAuth: registryAuth,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Close(); err != nil {
			lgr.Log("Failed to close image response:", err)
		}
	}()

	scanner := bufio.NewScanner(resp)
	var statuses []string
	for scanner.Scan() {
		var msg pullMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		switch {
		case msg.Error != "":
			return fmt.Errorf("image pull error: %s", msg.Error)
		case msg.Status != "":
			statuses = append(statuses, msg.Status)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(statuses) > 0 {
		lgr.Log("Image pull response:", strings.Join(statuses, "\n"))
	}
	return nil
}

func removeImage(ctx context.Context, lgr Logger, dc client.ImageAPIClient, imgName string) {
	lgr.Log("Removing image:", imgName)

	if _, err := dc.ImageRemove(ctx, imgName, image.RemoveOptions{Force: true, PruneChildren: true}); err != nil {
		lgr.Log("Failed to remove image: ", err.Error())
	}
}

func runImage(ctx context.Context, lgr Logger, dc client.ContainerAPIClient, imgName string,
	opts Options) (ContainerInfo, error) {
	c := ContainerInfo{Name: genContainerName(), ImageName: imgName}
	createResp, err := dc.ContainerCreate(ctx, &container.Config{
		Image:        imgName,
		Labels:       map[string]string{label: "true"},
		Env:          opts.env(),
		Entrypoint:   opts.Entrypoint,
		Cmd:          opts.Cmd,
		Volumes:      opts.volumes(),
		Hostname:     opts.Hostname,
		ExposedPorts: opts.ExposedPorts,
	}, &container.HostConfig{
		PublishAllPorts: true,
		PortBindings:    opts.PortBindings,
		ShmSize:         opts.ShmSize,
		Mounts:          opts.Mounts,
	}, &network.NetworkingConfig{},
		nil,
		c.Name)
	if err != nil {
		return c, err
	}
	c.ID = createResp.ID
	lgr.Log("Created container:", c.String())

	if err := dc.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		return c, err
	}
	lgr.Log("Started container:", c.String())

	if !opts.PortRequired {
		return c, nil
	}

	inspectResp, err := dc.ContainerInspect(ctx, c.ID)
	if err != nil {
		return c, err
	}
	lgr.Log("Inspected container:", c.String())

	if inspectResp.NetworkSettings == nil {
		return c, errNoNetworkSettings
	}
	c.Ports = inspectResp.NetworkSettings.Ports

	return c, nil
}

func stopContainer(ctx context.Context, lgr Logger, dc client.ContainerAPIClient, c ContainerInfo,
	logStdout, logStderr bool) {
	if logStdout || logStderr {
		if logs, err := dc.ContainerLogs(ctx, c.ID, container.LogsOptions{
			Timestamps: true, ShowStdout: logStdout, ShowStderr: logStderr,
		}); err == nil {
			b, err := io.ReadAll(logs)
			defer func() {
				if err := logs.Close(); err != nil {
					lgr.Log("Error closing logs:", err)
				}
			}()
			if err == nil {
				lgr.Log("Container logs:", string(b))
			} else {
				lgr.Log("Error reading container logs:", err)
			}
		} else {
			lgr.Log("Error fetching container logs:", err)
		}
	}

	if err := dc.ContainerStop(ctx, c.ID, container.StopOptions{}); err != nil {
		lgr.Log("Error stopping container:", c.String(), "error:", err)
	}
	lgr.Log("Stopped container:", c.String())

	if err := dc.ContainerRemove(ctx, c.ID,
		container.RemoveOptions{RemoveVolumes: true, Force: true}); err != nil {
		lgr.Log("Error removing container:", c.String(), "error:", err)
	}
	lgr.Log("Removed container:", c.String())
}

func waitContainerReady(ctx context.Context, lgr Logger, c ContainerInfo,
	readyFunc func(context.Context, ContainerInfo) bool, readyTimeout time.Duration) bool {
	if readyFunc == nil {
		return true
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ready := func() bool {
				readyCtx, canceledFunc := context.WithTimeout(ctx, readyTimeout)
				defer canceledFunc()
				return readyFunc(readyCtx, c)
			}()

			if ready {
				return true
			}
		case <-ctx.Done():
			lgr.Log("Container was never ready:", c.String())
			return false
		}
	}
}

func Run(t *testing.T, imgName string, opts Options, testFunc func(*testing.T, ContainerInfo)) {
	err := RunContext(context.Background(), t, imgName, opts, func(containerInfo ContainerInfo) error {
		testFunc(t, containerInfo)
		return nil
	})
	if err != nil {
		t.Fatal("Failed:", err)
	}
}

func RunContext(ctx context.Context, logger Logger, imgName string, opts Options, testFunc func(ContainerInfo) error) (retErr error) {
	dc, err := client.NewClientWithOpts(client.FromEnv, client.WithVersion("1.41"))
	if err != nil {
		return fmt.Errorf("error getting Docker client: %w", err)
	}
	defer func() {
		if err := dc.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("error closing Docker client: %w", err)
		}
	}()

	opts.init()
	pullCtx, pullTimeoutCancelFunc := context.WithTimeout(ctx, opts.PullTimeout)
	defer pullTimeoutCancelFunc()

	if err := pullImage(pullCtx, logger, dc, opts.PullRegistryAuth, imgName, opts.Platform); err != nil {
		return fmt.Errorf("error pulling image: %v error: %w", imgName, err)
	}

	return func() error {
		runCtx, runTimeoutCancelFunc := context.WithTimeout(ctx, opts.Timeout)
		defer runTimeoutCancelFunc()

		c, err := runImage(runCtx, logger, dc, imgName, opts)
		if err != nil {
			return fmt.Errorf("error running image: %v error: %w", imgName, err)
		}
		defer func() {
			stopCtx, stopTimeoutCancelFunc := context.WithTimeout(ctx, opts.CleanupTimeout)
			defer stopTimeoutCancelFunc()
			stopContainer(stopCtx, logger, dc, c, opts.LogStdout, opts.LogStderr)
			if opts.CleanupImage {
				removeImage(stopCtx, logger, dc, imgName)
			}
		}()

		if waitContainerReady(runCtx, logger, c, opts.ReadyFunc, opts.ReadyTimeout) {
			if err := testFunc(c); err != nil {
				return fmt.Errorf("error running test func: %w", err)
			}
		} else {
			return fmt.Errorf("timed out waiting for container to get ready: %v", c.String())
		}

		return nil
	}()
}
