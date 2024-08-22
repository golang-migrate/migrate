package redis

import (
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	neturl "net/url"
)

func queryContains(values neturl.Values, queries []string) bool {
	for _, query := range queries {
		if values.Has(query) {
			return true
		}
	}

	return false
}

var failoverSpecificQueries = []string{
	"sentinel_addr",
	"master_name",
	"sentinel_username",
	"sentinel_password",
	"replica_only",
	"use_disconnected_replicas",
}

var clusterSpecificQueries = []string{
	"addr",
	"max_redirects",
	"read_only",
	"route_by_latency",
	"route_randomly",
}

type queryOptions struct {
	q   neturl.Values
	err error
}

func (o *queryOptions) string(name string) string {
	vs := o.q[name]
	if len(vs) == 0 {
		return ""
	}
	delete(o.q, name) // enable detection of unknown parameters
	return vs[len(vs)-1]
}

func (o *queryOptions) strings(name string) []string {
	vs := o.q[name]
	delete(o.q, name)
	return vs
}

func (o *queryOptions) bool(name string) bool {
	switch s := o.string(name); s {
	case "true", "1":
		return true
	case "false", "0", "":
		return false
	default:
		if o.err == nil {
			o.err = fmt.Errorf("redis: invalid %s boolean: expected true/false/1/0 or an empty string, got %q", name, s)
		}
		return false
	}
}

func parseFailoverURL(url string) (*redis.FailoverOptions, error) {
	u, err := neturl.Parse(url)
	if err != nil {
		return nil, err
	}

	q := queryOptions{q: u.Query()}

	masterName := q.string("master_name")
	sentinelAddrs := q.strings("sentinel_addr")
	sentinelUsername := q.string("sentinel_username")
	sentinelPassword := q.string("sentinel_password")
	routeByLatency := q.bool("route_by_latency")
	routeRandomly := q.bool("route_randomly")
	replicaOnly := q.bool("replica_only")
	useDisconnectedReplicas := q.bool("use_disconnected_replicas")

	if len(sentinelAddrs) == 0 {
		return nil, errors.New("sentinel_addr is empty")
	}

	if q.err != nil {
		return nil, q.err
	}

	u.RawQuery = q.q.Encode()

	options, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, err
	}

	return &redis.FailoverOptions{
		MasterName:              masterName,
		SentinelAddrs:           sentinelAddrs,
		ClientName:              options.ClientName,
		SentinelUsername:        sentinelUsername,
		SentinelPassword:        sentinelPassword,
		RouteByLatency:          routeByLatency,
		RouteRandomly:           routeRandomly,
		ReplicaOnly:             replicaOnly,
		UseDisconnectedReplicas: useDisconnectedReplicas,
		Dialer:                  options.Dialer,
		OnConnect:               options.OnConnect,
		Protocol:                options.Protocol,
		Username:                options.Username,
		Password:                options.Password,
		DB:                      options.DB,
		MaxRetries:              options.MaxRetries,
		MinRetryBackoff:         options.MinRetryBackoff,
		MaxRetryBackoff:         options.MaxRetryBackoff,
		DialTimeout:             options.DialTimeout,
		ReadTimeout:             options.ReadTimeout,
		WriteTimeout:            options.WriteTimeout,
		ContextTimeoutEnabled:   options.ContextTimeoutEnabled,
		PoolFIFO:                options.PoolFIFO,
		PoolSize:                options.PoolSize,
		PoolTimeout:             options.PoolTimeout,
		MinIdleConns:            options.MinIdleConns,
		MaxIdleConns:            options.MaxIdleConns,
		MaxActiveConns:          options.MaxActiveConns,
		ConnMaxIdleTime:         options.ConnMaxIdleTime,
		ConnMaxLifetime:         options.ConnMaxLifetime,
		TLSConfig:               options.TLSConfig,
		DisableIndentity:        options.DisableIndentity,
		IdentitySuffix:          options.IdentitySuffix,
	}, nil
}

func determineMode(url string) (Mode, error) {
	u, err := neturl.Parse(url)
	if err != nil {
		return ModeUnspecified, err
	}

	values := u.Query()

	if queryContains(values, failoverSpecificQueries) {
		return ModeFailover, nil
	}

	if queryContains(values, clusterSpecificQueries) {
		return ModeCluster, nil
	}

	return ModeStandalone, nil
}
