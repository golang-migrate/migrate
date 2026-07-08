package dktest

import (
	"context"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/moby/moby/api/types/mount"
)

type Options struct {
	PullTimeout      time.Duration
	PullRegistryAuth string
	Timeout          time.Duration
	ReadyTimeout     time.Duration
	CleanupTimeout   time.Duration
	CleanupImage     bool
	ReadyFunc        func(context.Context, ContainerInfo) bool
	Env              map[string]string
	Entrypoint       []string
	Cmd              []string
	PortBindings     nat.PortMap
	PortRequired     bool
	LogStdout        bool
	LogStderr        bool
	ShmSize          int64
	Volumes          []string
	Mounts           []mount.Mount
	Hostname         string
	Platform         string
	ExposedPorts     nat.PortSet
}

func (o *Options) init() {
	if o.PullTimeout <= 0 {
		o.PullTimeout = DefaultPullTimeout
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = DefaultReadyTimeout
	}
	if o.CleanupTimeout <= 0 {
		o.CleanupTimeout = DefaultCleanupTimeout
	}
}

func (o *Options) volumes() map[string]struct{} {
	volumes := make(map[string]struct{})
	for _, v := range o.Volumes {
		volumes[v] = struct{}{}
	}
	return volumes
}

func (o *Options) env() []string {
	env := make([]string, 0, len(o.Env))
	for k, v := range o.Env {
		env = append(env, k+"="+v)
	}
	return env
}
