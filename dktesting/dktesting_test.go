package dktesting

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
)

// fakeRunContext replaces runContext until the test ends. Call n returns errs[n-1]. A nil error
// runs the test function in a container with the ID "container-<n>". It returns the call count.
func fakeRunContext(t *testing.T, errs ...error) *int {
	calls := 0
	runContext = func(ctx context.Context, lgr dktest.Logger, imgName string, opts dktest.Options,
		testFunc func(dktest.ContainerInfo) error) error {
		calls++
		if calls > len(errs) {
			t.Fatalf("container %d started, but only %d were expected", calls, len(errs))
		}
		if err := errs[calls-1]; err != nil {
			return err
		}
		return testFunc(dktest.ContainerInfo{ID: fmt.Sprint("container-", calls)})
	}
	t.Cleanup(func() { runContext = dktest.RunContext })
	return &calls
}

func TestRunWithRetry(t *testing.T) {
	notReady1 := fmt.Errorf("%s: container-1", notReadyErrText)
	notReady2 := fmt.Errorf("%s: container-2", notReadyErrText)
	startTimeout := fmt.Errorf("error running image: img error: %w", context.DeadlineExceeded)
	pullFailed := errors.New("error pulling image: img error: not found")

	testCases := []struct {
		name          string
		retryNotReady bool
		errs          []error
		wantRanIn     string // container the test function ran in, or "" if it did not run
		wantErrs      []error
	}{
		{name: "ready", retryNotReady: true, errs: []error{nil}, wantRanIn: "container-1"},
		{name: "not ready, then ready", retryNotReady: true, errs: []error{notReady1, nil}, wantRanIn: "container-2"},
		{name: "start timeout, then ready", retryNotReady: true, errs: []error{startTimeout, nil}, wantRanIn: "container-2"},
		{name: "never ready", retryNotReady: true, errs: []error{notReady1, notReady2}, wantErrs: []error{notReady1, notReady2}},
		{name: "other errors are not retried", retryNotReady: true, errs: []error{pullFailed}, wantErrs: []error{pullFailed}},
		{name: "no retry without RetryNotReady", retryNotReady: false, errs: []error{notReady1}, wantErrs: []error{notReady1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calls := fakeRunContext(t, tc.errs...)
			ranIn := ""
			err := runWithRetry(t, ContainerSpec{ImageName: "img", RetryNotReady: tc.retryNotReady},
				func(t *testing.T, c dktest.ContainerInfo) { ranIn = c.ID })

			if *calls != len(tc.errs) {
				t.Errorf("started %d containers, want %d", *calls, len(tc.errs))
			}
			if ranIn != tc.wantRanIn {
				t.Errorf("test function ran in %q, want %q", ranIn, tc.wantRanIn)
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

// TestNotReadyErrText checks that dktest still returns an error with notReadyErrText when a
// container does not get ready, so that RetryNotReady keeps working after a dktest upgrade.
func TestNotReadyErrText(t *testing.T) {
	opts := dktest.Options{
		Cmd:       []string{"sleep", "60"},
		ReadyFunc: func(context.Context, dktest.ContainerInfo) bool { return false },
		Timeout:   10 * time.Second,
	}
	err := dktest.RunContext(context.Background(), t, "alpine:3.20", opts, func(dktest.ContainerInfo) error {
		t.Error("the container must not get ready")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), notReadyErrText) {
		t.Fatalf("got error %v, want an error that contains %q", err, notReadyErrText)
	}
}
