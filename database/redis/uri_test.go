package redis

import (
	"github.com/redis/go-redis/v9"
	"reflect"
	"testing"
)

func TestParseFailoverURL(t *testing.T) {
	t.Parallel()

	url := `redis://:password@?sentinel_addr=sentinel_host:26379&master_name=mymaster`
	expectedOptions := &redis.FailoverOptions{
		SentinelAddrs: []string{"sentinel_host:26379"},
		Password:      "password",
		MasterName:    "mymaster",
	}

	actualOptions, err := parseFailoverURL(url)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(actualOptions, expectedOptions) {
		t.Fatalf("expected %+v,\ngot %+v", expectedOptions, actualOptions)
	}
}
