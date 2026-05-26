package dktest

import (
	"context"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/moby/moby/api/types/mount"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Options contains the configurable options for running tests in a Docker image.
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
	volumes := make(map[string]struct{}, len(o.Volumes))
	for _, volume := range o.Volumes {
		volumes[volume] = struct{}{}
	}
	return volumes
}

func (o *Options) env() []string {
	env := make([]string, 0, len(o.Env))
	for key, value := range o.Env {
		env = append(env, key+"="+value)
	}
	return env
}

func parsePlatform(raw string) (ocispec.Platform, bool) {
	parts := strings.Split(raw, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return ocispec.Platform{}, false
	}
	platform := ocispec.Platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		platform.Variant = parts[2]
	}
	return platform, true
}
