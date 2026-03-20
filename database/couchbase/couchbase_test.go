package couchbase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/dhui/dktest"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	testBucket   = "testmigrate"
	testUsername = "Administrator"
	testPassword = "password"
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		PullTimeout:  5 * time.Minute,
		Timeout:      120 * time.Second,
		ReadyTimeout: 60 * time.Second,
		LogStderr:    true,
		// Expose required ports: 8091 (mgmt), 8092 (views/capi), 8093 (query), 11210 (KV)
		PortBindings: nat.PortMap{
			"8091/tcp":  []nat.PortBinding{{}},
			"8092/tcp":  []nat.PortBinding{{}},
			"8093/tcp":  []nat.PortBinding{{}},
			"11210/tcp": []nat.PortBinding{{}},
		},
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "couchbase:community-7.6.2", Options: opts},
		{ImageName: "couchbase:community-8.0.0", Options: opts},
	}
)

func couchbaseConnectionString(host, port string) string {
	return fmt.Sprintf("couchbase://%s:%s@%s:%s/%s?x-scope=_default&network=external", testUsername, testPassword, host, port, testBucket)
}

// restReq sends an HTTP request to the Couchbase REST API.
func restReq(method, fullURL string, form url.Values, useAuth bool) ([]byte, error) {
	var bodyReader io.Reader
	if form != nil {
		bodyReader = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequest(method, fullURL, bodyReader)
	if err != nil {
		return nil, err
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if useAuth {
		req.SetBasicAuth(testUsername, testPassword)
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return body, fmt.Errorf("REST %s %s returned %d: %s", method, fullURL, resp.StatusCode, string(body))
	}
	return body, nil
}

// dockerExec runs a command inside a container and returns the output.
func dockerExec(ctx context.Context, containerID string, cmd []string) (string, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return "", err
	}
	defer func() { _ = dc.Close() }()

	exec, err := dc.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return "", fmt.Errorf("exec create: %w", err)
	}

	resp, err := dc.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", fmt.Errorf("exec attach: %w", err)
	}
	defer resp.Close()

	out, _ := io.ReadAll(resp.Reader)
	return string(out), nil
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, mgmtPort, err := c.Port(8091)
	if err != nil {
		return false
	}
	_, kvPort, err := c.Port(11210)
	if err != nil {
		return false
	}
	_, n1qlPort, err := c.Port(8093)
	if err != nil {
		return false
	}
	_, capiPort, err := c.Port(8092)
	if err != nil {
		return false
	}

	baseURL := fmt.Sprintf("http://%s:%s", ip, mgmtPort)

	// 1. Check if REST API is up (try without auth first, then with auth)
	_, err = restReq(http.MethodGet, baseURL+"/pools", nil, false)
	if err != nil {
		_, err = restReq(http.MethodGet, baseURL+"/pools", nil, true)
		if err != nil {
			log.Printf("REST not ready: %v", err)
			return false
		}
	}

	// 2. Provision the cluster via REST API
	if err := initCluster(ctx, c.ID, baseURL, ip, mgmtPort, kvPort, n1qlPort, capiPort); err != nil {
		log.Printf("init cluster error: %v", err)
		return false
	}

	// 3. Check bucket health via REST
	body, err := restReq(http.MethodGet, baseURL+"/pools/default/buckets/"+testBucket, nil, true)
	if err != nil {
		log.Printf("bucket not ready: %v", err)
		return false
	}

	var bucketInfo struct {
		Nodes []struct {
			Status string `json:"status"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(body, &bucketInfo); err != nil {
		log.Printf("parse bucket info: %v", err)
		return false
	}
	for _, node := range bucketInfo.Nodes {
		if node.Status != "healthy" {
			log.Printf("bucket node not healthy: %s", node.Status)
			return false
		}
	}

	// 4. Verify N1QL via REST
	n1qlURL := fmt.Sprintf("http://%s:%s/query/service", ip, n1qlPort)
	_, err = restReq(http.MethodPost, n1qlURL, url.Values{"statement": {"SELECT 1"}}, true)
	if err != nil {
		log.Printf("N1QL not ready: %v", err)
		return false
	}

	// 5. Verify gocb can connect and route queries via external network
	connStr := fmt.Sprintf("couchbase://%s:%s?network=external", ip, kvPort)
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: testUsername,
			Password: testPassword,
		},
	})
	if err != nil {
		log.Printf("gocb connect error: %v", err)
		return false
	}
	defer func() { _ = cluster.Close(nil) }()

	if err := cluster.WaitUntilReady(5*time.Second, &gocb.WaitUntilReadyOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeManagement, gocb.ServiceTypeQuery},
	}); err != nil {
		log.Printf("gocb cluster not ready: %v", err)
		return false
	}

	bucket := cluster.Bucket(testBucket)
	if err := bucket.WaitUntilReady(5*time.Second, nil); err != nil {
		log.Printf("gocb bucket not ready: %v", err)
		return false
	}

	_, err = cluster.Query("SELECT 1", &gocb.QueryOptions{Timeout: 5 * time.Second})
	if err != nil {
		log.Printf("gocb N1QL not ready: %v", err)
		return false
	}

	return true
}

// initCluster provisions the Couchbase cluster via the REST API.
// Alternate addresses are set using couchbase-cli inside the container
// (the REST API doesn't reliably set N1QL alternate ports).
func initCluster(ctx context.Context, containerID, baseURL, host, mgmtPort, kvPort, n1qlPort, capiPort string) error {
	// Check if cluster is already initialized
	_, err := restReq(http.MethodGet, baseURL+"/pools/default", nil, true)
	alreadyInitialized := err == nil

	if !alreadyInitialized {
		// Fresh node — no auth required for initial setup
		if _, err := restReq(http.MethodPost, baseURL+"/node/controller/setupServices",
			url.Values{"services": {"kv,n1ql,index"}}, false); err != nil {
			return fmt.Errorf("setup services: %w", err)
		}
		if _, err := restReq(http.MethodPost, baseURL+"/pools/default",
			url.Values{"memoryQuota": {"256"}, "indexMemoryQuota": {"256"}}, false); err != nil {
			return fmt.Errorf("set memory quota: %w", err)
		}
		if _, err := restReq(http.MethodPost, baseURL+"/settings/web",
			url.Values{"username": {testUsername}, "password": {testPassword}, "port": {"8091"}}, false); err != nil {
			return fmt.Errorf("set credentials: %w", err)
		}
	}

	// Set alternate addresses using couchbase-cli inside the container
	// (this reliably sets all service ports including n1ql, unlike the REST API)
	portsArg := fmt.Sprintf("mgmt=%s,kv=%s,capi=%s,n1ql=%s", mgmtPort, kvPort, capiPort, n1qlPort)
	out, err := dockerExec(ctx, containerID, []string{
		"couchbase-cli", "setting-alternate-address",
		"-c", "127.0.0.1:8091",
		"--username", testUsername,
		"--password", testPassword,
		"--set",
		"--node", "127.0.0.1",
		"--hostname", host,
		"--ports", portsArg,
	})
	if err != nil {
		return fmt.Errorf("set alternate addresses: %w (output: %s)", err, out)
	}

	// Create the test bucket (idempotent)
	_, err = restReq(http.MethodPost, baseURL+"/pools/default/buckets",
		url.Values{"name": {testBucket}, "ramQuota": {"256"}, "bucketType": {"couchbase"}}, true)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("create bucket: %w", err)
	}

	// Give the bucket time to initialize on first creation
	if !alreadyInitialized {
		time.Sleep(3 * time.Second)
	}

	return nil
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testWithInstance", testWithInstance)
	t.Run("testLockWorks", testLockWorks)

	// Note: we intentionally do not clean up images here.
	// Containers are cleaned up by dktest automatically.
	// Removing images would force slow re-pulls on every test run.
}

func test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, kvPort, err := c.Port(11210)
		if err != nil {
			t.Fatal(err)
		}

		addr := couchbaseConnectionString(ip, kvPort)
		p := &Couchbase{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		migration := []byte(`[{"query":"SELECT 1"}]`)
		dt.Test(t, d, migration)
	})
}

func testWithInstance(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, kvPort, err := c.Port(11210)
		if err != nil {
			t.Fatal(err)
		}

		connStr := fmt.Sprintf("couchbase://%s:%s?network=external", ip, kvPort)
		cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: testUsername,
				Password: testPassword,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		d, err := WithInstance(cluster, &Config{
			BucketName: testBucket,
			ScopeName:  "_default",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.TestNilVersion(t, d)
		dt.TestLockAndUnlock(t, d)

		migration := []byte(`[{"query":"SELECT 1"}]`)
		dt.TestRun(t, d, bytes.NewReader(migration))
		dt.TestSetVersion(t, d)
		dt.TestDrop(t, d)
	})
}

func testLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, kvPort, err := c.Port(11210)
		if err != nil {
			t.Fatal(err)
		}

		addr := couchbaseConnectionString(ip, kvPort)
		p := &Couchbase{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		migration := []byte(`[{"query":"SELECT 1"}]`)
		dt.TestRun(t, d, bytes.NewReader(migration))

		cb := d.(*Couchbase)

		// Test lock/unlock cycle
		if err := cb.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := cb.Unlock(); err != nil {
			t.Fatal(err)
		}

		if err := cb.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := cb.Unlock(); err != nil {
			t.Fatal(err)
		}

		// Enable locking with a short timeout and test lock conflict
		cb.config.Locking.Enabled = true
		cb.config.Locking.Timeout = 1
		if err := cb.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := cb.Lock(); err == nil {
			t.Fatal("should have failed, couchbase should be locked already")
		}
	})
}
