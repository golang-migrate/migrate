package dktest

import (
	"net/netip"

	"github.com/docker/go-connections/nat"
	"github.com/moby/moby/api/types/network"
)

func toNetworkPortMap(portMap nat.PortMap) (network.PortMap, error) {
	networkPortMap := make(network.PortMap, len(portMap))
	for port, bindings := range portMap {
		networkPort, err := network.ParsePort(string(port))
		if err != nil {
			return nil, err
		}

		networkBindings := make([]network.PortBinding, 0, len(bindings))
		for _, binding := range bindings {
			networkBinding := network.PortBinding{HostPort: binding.HostPort}
			if binding.HostIP != "" {
				if hostIP, err := netip.ParseAddr(binding.HostIP); err == nil {
					networkBinding.HostIP = hostIP
				}
			}
			networkBindings = append(networkBindings, networkBinding)
		}
		networkPortMap[networkPort] = networkBindings
	}
	return networkPortMap, nil
}

func toNetworkPortSet(portSet nat.PortSet) (network.PortSet, error) {
	networkPortSet := make(network.PortSet, len(portSet))
	for port := range portSet {
		networkPort, err := network.ParsePort(string(port))
		if err != nil {
			return nil, err
		}
		networkPortSet[networkPort] = struct{}{}
	}
	return networkPortSet, nil
}

func fromNetworkPortMap(portMap network.PortMap) nat.PortMap {
	natPortMap := make(nat.PortMap, len(portMap))
	for port, bindings := range portMap {
		natPort, err := nat.NewPort(string(port.Proto()), port.Port())
		if err != nil {
			continue
		}

		natBindings := make([]nat.PortBinding, 0, len(bindings))
		for _, binding := range bindings {
			hostIP := ""
			if binding.HostIP.IsValid() {
				hostIP = binding.HostIP.String()
			}
			natBindings = append(natBindings, nat.PortBinding{
				HostIP:   hostIP,
				HostPort: binding.HostPort,
			})
		}
		natPortMap[natPort] = natBindings
	}
	return natPortMap
}
