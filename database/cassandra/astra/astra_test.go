package cassandra

import (
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/gocql/gocql"
	cas "github.com/golang-migrate/migrate/v4/database/cassandra"
)

func TestAstra(t *testing.T) {
	type mockResult struct {
		timeout time.Duration

		// NewClusterFromBundle
		path     string
		username string
		password string

		// NewClusterFromURL
		apiUrl     string
		databaseID string
		token      string
	}

	var (
		errNewClusterFromBundle = errors.New("NewClusterFromBundle")
		errNewClusterFromURL    = errors.New("NewClusterFromURL")
	)

	test := func(t *testing.T, url string) (mockResult, error) {
		t.Helper()

		var mr mockResult

		// Since we can't actually call the Astra API, we mock the calls and return an error so we never dial.
		cas.GocqlastraNewClusterFromBundle = func(path string, username string, password string, timeout time.Duration) (*gocql.ClusterConfig, error) {
			mr.path = path
			mr.username = username
			mr.password = password
			mr.timeout = timeout
			return nil, errNewClusterFromBundle
		}

		cas.GocqlastraNewClusterFromURL = func(apiUrl string, databaseID string, token string, timeout time.Duration) (*gocql.ClusterConfig, error) {
			mr.apiUrl = apiUrl
			mr.databaseID = databaseID
			mr.token = token
			mr.timeout = timeout
			return nil, errNewClusterFromURL
		}

		astra := &Astra{}

		_, err := astra.Open(url)
		return mr, err
	}

	t.Run("Token", func(t *testing.T) {
		mr, err := test(t, "astra:///testks?token=token&database_id=database_id")
		if err != errNewClusterFromURL {
			t.Error("Expected", errNewClusterFromURL, "but got", err)
		}
		if mr.token != "token" {
			t.Error("Expected token to be 'token' but got", mr.token)
		}
		if mr.databaseID != "database_id" {
			t.Error("Expected database_id to be 'database_id' but got", mr.databaseID)
		}
	})
	t.Run("Bundle", func(t *testing.T) {
		mr, err := test(t, "astra:///testks?bundle=bundle.zip&token=AstraCS:password")
		if err != errNewClusterFromBundle {
			t.Error("Expected", errNewClusterFromBundle, "but got", err)
		}
		if mr.path != "bundle.zip" {
			t.Error("Expected path to be 'bundle.zip' but got", mr.path)
		}
		if mr.username != "token" {
			t.Error("Expected username to be 'token' but got", mr.username)
		}
		if mr.password != "AstraCS:password" {
			t.Error("Expected password to be 'AstraCS:password' but got", mr.password)
		}
	})

	t.Run("No Keyspace", func(t *testing.T) {
		astra := &Astra{}
		_, err := astra.Open("astra://")
		if err != cas.ErrNoKeyspace {
			t.Error("Expected", cas.ErrNoKeyspace, "but got", err)
		}
	})

	t.Run("AstraMissing", func(t *testing.T) {
		astra := &Astra{}
		_, err := astra.Open("astra:///testks")
		if err != cas.ErrAstraMissing {
			t.Error("Expected", cas.ErrAstraMissing, "but got", err)
		}
	})
	t.Run("No Token", func(t *testing.T) {
		astra := &Astra{}
		_, err := astra.Open("astra:///testks?database_id=database_id")
		if err != cas.ErrAstraMissing {
			t.Error("Expected", cas.ErrAstraMissing, "but got", err)
		}
	})
	t.Run("No DatabaseID", func(t *testing.T) {
		astra := &Astra{}
		_, err := astra.Open("astra:///testks?token=AstraCS:password")
		if err != cas.ErrAstraMissing {
			t.Error("Expected", cas.ErrAstraMissing, "but got", err)
		}
	})
	t.Run("No Bundle", func(t *testing.T) {
		astra := &Astra{}
		_, err := astra.Open("astra:///testks?token=AstraCS:password")
		if err != cas.ErrAstraMissing {
			t.Error("Expected", cas.ErrAstraMissing, "but got", err)
		}
	})
	t.Run("Custom API URL", func(t *testing.T) {
		mr, err := test(t, "astra:///testks?token=token&database_id=database_id&api_url=api_url")
		if err != errNewClusterFromURL {
			t.Error("Expected", errNewClusterFromURL, "but got", err)
		}
		if mr.apiUrl != "api_url" {
			t.Error("Expected api_url to be 'api_url' but got", mr.apiUrl)
		}
	})
}

func TestTripleSlashInURLMeansNoHost(t *testing.T) {
	const str = "astra:///testks?token=token&database_id=database_id"
	u, err := url.Parse(str)
	if err != nil {
		t.Fatal(err)
	}
	if u.Host != "" {
		t.Error("Expected host to be empty but got", u.Host)
	}
	if u.Path != "/testks" {
		t.Error("Expected path to be '/testks' but got", u.Path)
	}
}
