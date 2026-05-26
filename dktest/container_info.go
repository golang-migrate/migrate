package dktest

import (
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
)

// ContainerInfo holds information about a running Docker container.
type ContainerInfo struct {
	ID        string
	Name      string
	ImageName string
	Ports     nat.PortMap
}

func (c ContainerInfo) String() string {
	return fmt.Sprintf("dktest.ContainerInfo{ID:%q, Name:%q, ImageName:%q}", c.ID, c.Name, c.ImageName)
}

// Port gets the specified published TCP port.
func (c ContainerInfo) Port(containerPort uint16) (hostIP string, hostPort string, err error) {
	port, err := nat.NewPort("tcp", strconv.Itoa(int(containerPort)))
	if err != nil {
		return "", "", err
	}
	return mapPort(c.Ports, port)
}

// UDPPort gets the specified published UDP port.
func (c ContainerInfo) UDPPort(containerPort uint16) (hostIP string, hostPort string, err error) {
	port, err := nat.NewPort("udp", strconv.Itoa(int(containerPort)))
	if err != nil {
		return "", "", err
	}
	return mapPort(c.Ports, port)
}

// FirstPort gets the first published TCP port.
func (c ContainerInfo) FirstPort() (hostIP string, hostPort string, err error) {
	return firstPort(c.Ports, "tcp")
}

// FirstUDPPort gets the first published UDP port.
func (c ContainerInfo) FirstUDPPort() (hostIP string, hostPort string, err error) {
	return firstPort(c.Ports, "udp")
}

func mapPort(portMap nat.PortMap, port nat.Port) (hostIP string, hostPort string, err error) {
	if bindings, ok := portMap[port]; ok {
		for _, binding := range bindings {
			return mapHost(binding.HostIP), binding.HostPort, nil
		}
	}

	portInt := port.Int()
	proto := port.Proto()
	for p, bindings := range portMap {
		if p.Proto() != proto {
			continue
		}
		start, end, err := p.Range()
		if err != nil || portInt < start || portInt > end {
			continue
		}
		offset := portInt - start
		if offset >= len(bindings) {
			continue
		}
		binding := bindings[offset]
		return mapHost(binding.HostIP), binding.HostPort, nil
	}
	return "", "", errNoPort
}

func firstPort(portMap nat.PortMap, proto string) (hostIP string, hostPort string, err error) {
	for port, bindings := range portMap {
		if port.Proto() != proto {
			continue
		}
		for _, binding := range bindings {
			return mapHost(binding.HostIP), binding.HostPort, nil
		}
	}
	return "", "", errNoPort
}

func mapHost(host string) string {
	switch host {
	case "", "0.0.0.0":
		return "127.0.0.1"
	default:
		return host
	}
}
