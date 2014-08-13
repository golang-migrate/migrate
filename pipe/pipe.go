// Package pipe has functions for pipe channel handling.
package pipe

import (
	"os"
	"os/signal"
)

// New creates a new pipe. A pipe is basically a channel.
func New() chan interface{} {
	return make(chan interface{}, 0)
}

// Close closes a pipe and optionally sends an error
func Close(pipe chan interface{}, err error) {
	if err != nil {
		pipe <- err
	}
	close(pipe)
}

func Signal() chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	return c
}

func WaitAndRedirect(pipe, redirectPipe chan interface{}, signal chan os.Signal) (ok bool) {
	errorReceived := false
	signalsReceived := 0

	if pipe != nil && redirectPipe != nil {
		for {
			select {

			case <-signal:
				signalsReceived += 1
				if signalsReceived > 1 {
					os.Exit(5)
				} else {
					redirectPipe <- " Aborting after this migration ..."
				}

			case item, ok := <-pipe:
				if !ok {
					return !errorReceived && signalsReceived == 0
				} else {
					redirectPipe <- item
					switch item.(type) {
					case error:
						errorReceived = true
					}
				}
			}
		}
	}
	return !errorReceived && signalsReceived == 0
}

// ReadErrors selects all received errors and returns them.
// This is helpful for synchronous migration functions.
func ReadErrors(pipe chan interface{}) []error {
	err := make([]error, 0)
	if pipe != nil {
		for {
			select {
			case item, ok := <-pipe:
				if !ok {
					return err
				} else {
					switch item.(type) {
					case error:
						err = append(err, item.(error))
					}
				}
			}
		}
	}
	return err
}
