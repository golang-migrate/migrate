package dktesting

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStartWithRetry(t *testing.T) {
	notReady1 := fmt.Errorf("container 1: %w", errNotReady)
	notReady2 := fmt.Errorf("container 2: %w", errNotReady)
	createFailed := errors.New("creating container: no such image")
	startTimeout := fmt.Errorf("starting container: %w", context.DeadlineExceeded)

	testCases := []struct {
		name     string
		attempts int
		errs     []error // error of each start; nil means the container got ready
		wantID   string  // container that got ready, or "" if none did
		wantErrs []error
	}{
		{name: "ready", attempts: 2, errs: []error{nil}, wantID: "container-1"},
		{name: "not ready, then ready", attempts: 2, errs: []error{notReady1, nil}, wantID: "container-2"},
		{name: "never ready", attempts: 2, errs: []error{notReady1, notReady2}, wantErrs: []error{notReady1, notReady2}},
		{name: "other errors are not retried", attempts: 2, errs: []error{createFailed}, wantErrs: []error{createFailed}},
		{name: "start timeouts are not retried", attempts: 2, errs: []error{startTimeout}, wantErrs: []error{startTimeout}},
		{name: "no retry with one attempt", attempts: 1, errs: []error{notReady1}, wantErrs: []error{notReady1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			starts := 0
			c, err := startWithRetry(t, tc.attempts, func() (ContainerInfo, error) {
				starts++
				if starts > len(tc.errs) {
					t.Fatalf("container %d started, but only %d were expected", starts, len(tc.errs))
				}
				if err := tc.errs[starts-1]; err != nil {
					return ContainerInfo{}, err
				}
				return ContainerInfo{ID: fmt.Sprint("container-", starts)}, nil
			})

			if starts != len(tc.errs) {
				t.Errorf("started %d containers, want %d", starts, len(tc.errs))
			}
			if c.ID != tc.wantID {
				t.Errorf("got container %q, want %q", c.ID, tc.wantID)
			}
			if len(tc.wantErrs) == 0 && err != nil {
				t.Errorf("got error %v, want no error", err)
			}
			for _, want := range tc.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("got error %v, want it to contain %v", err, want)
				}
			}
		})
	}
}

// TestStartReadyNotReady checks with real containers that startReady returns
// errNotReady, which startWithRetry retries.
func TestStartReadyNotReady(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	const image = "alpine:3.20"
	dc, err := newClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dc.Close() }()
	if err := pullOnce(dc, image, defaultPullTimeout); err != nil {
		t.Fatal(err)
	}
	neverReady := func(context.Context, ContainerInfo) bool { return false }

	testCases := []struct {
		name    string
		cmd     []string
		maxTime time.Duration
	}{
		{name: "times out", cmd: []string{"sleep", "60"}, maxTime: 10 * time.Second},
		{name: "exits", cmd: []string{"true"}, maxTime: 5 * time.Second},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			_, err := startReady(t, dc, image, Options{Cmd: tc.cmd, ReadyFunc: neverReady, Timeout: 3 * time.Second})
			if !errors.Is(err, errNotReady) {
				t.Fatalf("got error %v, want errNotReady", err)
			}
			if elapsed := time.Since(start); elapsed > tc.maxTime {
				t.Errorf("took %v, want at most %v", elapsed, tc.maxTime)
			}
			t.Log(err)
		})
	}
}
