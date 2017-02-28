// Package testing is used in driver tests.
package testing

import (
	"bufio"
	"context" // TODO: is issue with go < 1.7?
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	dockertypes "github.com/docker/docker/api/types"
	dockercontainer "github.com/docker/docker/api/types/container"
	dockernetwork "github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
)

func NewDockerContainer(t testing.TB, image string, env []string) (*DockerContainer, error) {
	c, err := dockerclient.NewEnvClient()
	if err != nil {
		return nil, err
	}

	contr := &DockerContainer{
		t:         t,
		client:    c,
		ImageName: image,
		ENV:       env,
	}

	if err := contr.PullImage(); err != nil {
		return nil, err
	}

	if err := contr.Start(); err != nil {
		return nil, err
	}

	return contr, nil
}

// DockerContainer implements Instance interface
type DockerContainer struct {
	t                  testing.TB
	client             *dockerclient.Client
	ImageName          string
	ENV                []string
	ContainerId        string
	ContainerName      string
	ContainerJSON      dockertypes.ContainerJSON
	containerInspected bool
	keepForDebugging   bool
}

func (d *DockerContainer) PullImage() error {
	d.t.Logf("Docker: Pull image %v", d.ImageName)
	r, err := d.client.ImagePull(context.Background(), d.ImageName, dockertypes.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer r.Close()

	// read output and log relevant lines
	bf := bufio.NewScanner(r)
	for bf.Scan() {
		var resp dockerImagePullOutput
		if err := json.Unmarshal(bf.Bytes(), &resp); err != nil {
			return err
		}
		if strings.HasPrefix(resp.Status, "Status: ") {
			d.t.Logf("Docker: %v", resp.Status)
		}
	}
	return bf.Err()
}

func (d *DockerContainer) Start() error {
	containerName := fmt.Sprintf("migrate_test_%v", pseudoRandStr(10))

	// create container first
	resp, err := d.client.ContainerCreate(context.Background(),
		&dockercontainer.Config{
			Image:  d.ImageName,
			Labels: map[string]string{"migrate_test": "true"},
			Env:    d.ENV,
		},
		&dockercontainer.HostConfig{
			PublishAllPorts: true,
		},
		&dockernetwork.NetworkingConfig{},
		containerName)
	if err != nil {
		return err
	}

	d.ContainerId = resp.ID
	d.ContainerName = containerName

	// then start it
	if err := d.client.ContainerStart(context.Background(), resp.ID, dockertypes.ContainerStartOptions{}); err != nil {
		return err
	}

	d.t.Logf("Docker: Started container %v (%v) for image %v listening at %v:%v", resp.ID[0:12], containerName, d.ImageName, d.Host(), d.Port())
	for _, v := range resp.Warnings {
		d.t.Logf("Docker: Warning: %v", v)
	}
	return nil
}

func (d *DockerContainer) KeepForDebugging() {
	d.keepForDebugging = true
}

func (d *DockerContainer) Remove() error {
	if d.keepForDebugging {
		return nil
	}

	if len(d.ContainerId) == 0 {
		return fmt.Errorf("missing containerId")
	}
	if err := d.client.ContainerRemove(context.Background(), d.ContainerId,
		dockertypes.ContainerRemoveOptions{
			Force: true,
		}); err != nil {
		d.t.Log(err)
		return err
	}
	d.t.Logf("Docker: Removed %v", d.ContainerName)
	return nil
}

func (d *DockerContainer) Inspect() error {
	if len(d.ContainerId) == 0 {
		return fmt.Errorf("missing containerId")
	}
	resp, err := d.client.ContainerInspect(context.Background(), d.ContainerId)
	if err != nil {
		return err
	}

	d.ContainerJSON = resp
	d.containerInspected = true
	return nil
}

func (d *DockerContainer) Logs() (io.ReadCloser, error) {
	if len(d.ContainerId) == 0 {
		return nil, fmt.Errorf("missing containerId")
	}

	return d.client.ContainerLogs(context.Background(), d.ContainerId, dockertypes.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
}

func (d *DockerContainer) firstPortMapping() (containerPort uint, hostIP string, hostPort uint, err error) {
	if !d.containerInspected {
		if err := d.Inspect(); err != nil {
			d.t.Fatal(err)
		}
	}

	for port, bindings := range d.ContainerJSON.NetworkSettings.Ports {
		for _, binding := range bindings {

			hostPortUint, err := strconv.ParseUint(binding.HostPort, 10, 64)
			if err != nil {
				return 0, "", 0, err
			}

			return uint(port.Int()), binding.HostIP, uint(hostPortUint), nil
		}
	}
	return 0, "", 0, fmt.Errorf("no port binding")
}

func (d *DockerContainer) Host() string {
	_, hostIP, _, err := d.firstPortMapping()
	if err != nil {
		d.t.Fatal(err)
	}

	if hostIP == "0.0.0.0" {
		return "127.0.0.1"
	} else {
		return hostIP
	}
}

func (d *DockerContainer) Port() uint {
	_, _, port, err := d.firstPortMapping()
	if err != nil {
		d.t.Fatal(err)
	}
	return port
}

type dockerImagePullOutput struct {
	Status          string `json:"status"`
	ProgressDetails struct {
		Current int `json:"current"`
		Total   int `json:"total"`
	} `json:"progressDetail"`
	Id       string `json:"id"`
	Progress string `json:"progress"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func pseudoRandStr(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
