package pipe

// New creates a new pipe. A pipe is basically a channel.
func New() chan interface{} {
	return make(chan interface{}, 0)
}

// Close closes pipe and optionally sends an error
func Close(pipe chan interface{}, err error) {
	if err != nil {
		pipe <- err
	}
	close(pipe)
}

// WaitAndRedirect waits for pipe to be closed and
// redirects all messages from pipe to redirectPipe
// while it waits.
func WaitAndRedirect(pipe, redirectPipe chan interface{}) {
	if pipe != nil && redirectPipe != nil {
		for {
			select {
			case item, ok := <-pipe:
				if !ok {
					return
				} else {
					redirectPipe <- item
				}
			}
		}
	}
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
