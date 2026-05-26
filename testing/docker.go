// Package testing is used in driver tests and should only be used by migrate tests.
//
// Deprecated: If you'd like to test using Docker images, use package github.com/golang-migrate/migrate/v4/dktest instead.
package testing

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
)

func NewDockerContainer(t testing.TB, image string, env []string, cmd []string) (*DockerContainer, error) {
	c, err := dockerclient.NewClientWithOpts(
		dockerclient.FromEnv,
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, err
	}

	if cmd == nil {
		cmd = make([]string, 0)
	}

	contr := &DockerContainer{
		t:         t,
		client:    c,
		ImageName: image,
		ENV:       env,
		Cmd:       cmd,
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
	Cmd                []string
	ContainerId        string
	ContainerName      string
	ContainerJSON      dockercontainer.InspectResponse
	containerInspected bool
	keepForDebugging   bool
}

func (d *DockerContainer) PullImage() (err error) {
	if d == nil {
		return errors.New("cannot pull image on a nil *DockerContainer")
	}
	d.t.Logf("Docker: Pull image %v", d.ImageName)
	r, err := d.client.ImagePull(context.Background(), d.ImageName, dockerclient.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if errClose := r.Close(); errClose != nil {
			err = errors.Join(err, errClose)
		}
	}()

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
	if d == nil {
		return errors.New("cannot start a nil *DockerContainer")
	}

	containerName := fmt.Sprintf("migrate_test_%s", pseudoRandStr(10))

	// create container first
	resp, err := d.client.ContainerCreate(context.Background(), dockerclient.ContainerCreateOptions{
		Config: &dockercontainer.Config{
			Image:  d.ImageName,
			Labels: map[string]string{"migrate_test": "true"},
			Env:    d.ENV,
			Cmd:    d.Cmd,
		},
		HostConfig: &dockercontainer.HostConfig{
			PublishAllPorts: true,
		},
		NetworkingConfig: &dockernetwork.NetworkingConfig{},
		Name:             containerName,
	})
	if err != nil {
		return err
	}

	d.ContainerId = resp.ID
	d.ContainerName = containerName

	// then start it
	if _, err := d.client.ContainerStart(context.Background(), resp.ID, dockerclient.ContainerStartOptions{}); err != nil {
		return err
	}

	d.t.Logf("Docker: Started container %v (%v) for image %v listening at %v:%v", resp.ID[0:12], containerName, d.ImageName, d.Host(), d.Port())
	for _, v := range resp.Warnings {
		d.t.Logf("Docker: Warning: %v", v)
	}
	return nil
}

func (d *DockerContainer) KeepForDebugging() {
	if d == nil {
		return
	}

	d.keepForDebugging = true
}

func (d *DockerContainer) Remove() error {
	if d == nil {
		return errors.New("cannot remove a nil *DockerContainer")
	}

	if d.keepForDebugging {
		return nil
	}

	if len(d.ContainerId) == 0 {
		return errors.New("missing containerId")
	}
	if _, err := d.client.ContainerRemove(context.Background(), d.ContainerId,
		dockerclient.ContainerRemoveOptions{
			Force: true,
		}); err != nil {
		d.t.Log(err)
		return err
	}
	d.t.Logf("Docker: Removed %v", d.ContainerName)
	return nil
}

func (d *DockerContainer) Inspect() error {
	if d == nil {
		return errors.New("cannot inspect a nil *DockerContainer")
	}

	if len(d.ContainerId) == 0 {
		return errors.New("missing containerId")
	}
	resp, err := d.client.ContainerInspect(context.Background(), d.ContainerId, dockerclient.ContainerInspectOptions{})
	if err != nil {
		return err
	}

	d.ContainerJSON = resp.Container
	d.containerInspected = true
	return nil
}

func (d *DockerContainer) Logs() (io.ReadCloser, error) {
	if d == nil {
		return nil, errors.New("cannot view logs for a nil *DockerContainer")
	}
	if len(d.ContainerId) == 0 {
		return nil, errors.New("missing containerId")
	}

	return d.client.ContainerLogs(context.Background(), d.ContainerId, dockerclient.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
}

func (d *DockerContainer) portMapping(selectFirst bool, cPort int) (hostIP string, hostPort uint, err error) {
	if !d.containerInspected {
		if err := d.Inspect(); err != nil {
			d.t.Fatal(err)
		}
	}

	for port, bindings := range d.ContainerJSON.NetworkSettings.Ports {
		if !selectFirst && int(port.Num()) != cPort {
			// Skip ahead until we find the port we want
			continue
		}
		if len(bindings) > 0 {
			binding := bindings[0]
			hostPortUint, err := strconv.ParseUint(binding.HostPort, 10, 64)
			if err != nil {
				return "", 0, err
			}
			hostIP := ""
			if binding.HostIP.IsValid() {
				hostIP = binding.HostIP.String()
			}
			return hostIP, uint(hostPortUint), nil
		}
	}

	if selectFirst {
		return "", 0, errors.New("no port binding")
	} else {
		return "", 0, errors.New("specified port not bound")
	}
}

func (d *DockerContainer) Host() string {
	if d == nil {
		panic("Cannot get host for a nil *DockerContainer")
	}
	hostIP, _, err := d.portMapping(true, -1)
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
	if d == nil {
		panic("Cannot get port for a nil *DockerContainer")
	}
	_, port, err := d.portMapping(true, -1)
	if err != nil {
		d.t.Fatal(err)
	}
	return port
}

func (d *DockerContainer) PortFor(cPort int) uint {
	if d == nil {
		panic("Cannot get port for a nil *DockerContainer")
	}
	_, port, err := d.portMapping(false, cPort)
	if err != nil {
		d.t.Fatal(err)
	}
	return port
}

func (d *DockerContainer) NetworkSettings() dockercontainer.NetworkSettings {
	if d == nil {
		panic("Cannot get network settings for a nil *DockerContainer")
	}
	netSettings := d.ContainerJSON.NetworkSettings
	return *netSettings
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

func pseudoRandStr(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.IntN(len(letterRunes))]
	}
	return string(b)
}
