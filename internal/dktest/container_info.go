package dktest

import (
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
)

func mapHost(h string) string {
	switch h {
	case "", "0.0.0.0":
		return "127.0.0.1"
	default:
		return h
	}
}

func mapPort(portMap nat.PortMap, port nat.Port) (hostIP string, hostPort string, err error) {
	portBindings, ok := portMap[port]
	if ok {
		for _, pb := range portBindings {
			return mapHost(pb.HostIP), pb.HostPort, nil
		}
	}

	portInt := port.Int()
	proto := port.Proto()
	for p, portBindings := range portMap {
		if p.Proto() != proto {
			continue
		}
		start, end, err := p.Range()
		if err != nil {
			continue
		}
		if portInt < start || portInt > end {
			continue
		}
		offset := portInt - start
		if offset >= len(portBindings) {
			continue
		}
		pb := portBindings[offset]
		return mapHost(pb.HostIP), pb.HostPort, nil
	}

	return "", "", errNoPort
}

func firstPort(portMap nat.PortMap, proto string) (hostIP string, hostPort string, err error) {
	for p, portBindings := range portMap {
		if p.Proto() != proto {
			continue
		}
		for _, pb := range portBindings {
			return mapHost(pb.HostIP), pb.HostPort, nil
		}
	}
	return "", "", errNoPort
}

func portMapToStrings(portMap nat.PortMap) []string {
	var portBindingStrs []string
	ports := make([]nat.Port, 0, len(portMap))
	for p := range portMap {
		ports = append(ports, p)
	}

	nat.SortPortMap(ports, portMap)

	for _, p := range ports {
		start, end, err := p.Range()
		if err != nil {
			continue
		}

		portBindings, ok := portMap[p]
		if !ok {
			continue
		}

		l := min(end-start+1, len(portBindings))
		proto := p.Proto()
		for i := 0; i < l; i++ {
			pb := portBindings[i]
			portBindingStrs = append(portBindingStrs, strconv.Itoa(start+i)+"/"+proto+" -> "+
				pb.HostIP+":"+pb.HostPort)
		}
	}
	return portBindingStrs
}

type ContainerInfo struct {
	ID        string
	Name      string
	ImageName string
	Ports     nat.PortMap
}

func (c ContainerInfo) String() string {
	return fmt.Sprintf("dktest.ContainerInfo{ID:%q, Name:%q, ImageName:%q, Ports:%v}", c.ID, c.Name, c.ImageName,
		portMapToStrings(c.Ports))
}

func (c ContainerInfo) Port(containerPort uint16) (hostIP string, hostPort string, err error) {
	port, err := nat.NewPort("tcp", strconv.Itoa(int(containerPort)))
	if err != nil {
		return "", "", err
	}
	return mapPort(c.Ports, port)
}

func (c ContainerInfo) UDPPort(containerPort uint16) (hostIP string, hostPort string, err error) {
	port, err := nat.NewPort("udp", strconv.Itoa(int(containerPort)))
	if err != nil {
		return "", "", err
	}
	return mapPort(c.Ports, port)
}

func (c ContainerInfo) FirstPort() (hostIP string, hostPort string, err error) {
	return firstPort(c.Ports, "tcp")
}

func (c ContainerInfo) FirstUDPPort() (hostIP string, hostPort string, err error) {
	return firstPort(c.Ports, "udp")
}
