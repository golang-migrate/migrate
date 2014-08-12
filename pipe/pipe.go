package pipe

func New() chan interface{} {
	return make(chan interface{}, 0)
}

func Close(pipe chan interface{}, err error) {
	if err != nil {
		pipe <- err
	}
	close(pipe)
}

func Redirect(fromPipe, toPipe chan interface{}) {
	if fromPipe != nil && toPipe != nil {
		for {
			select {
			case item, ok := <-fromPipe:
				if !ok {
					return
				} else {
					toPipe <- item
				}
			}
		}
	}
}

func Wait(pipe chan interface{}) {
	if pipe != nil {
		for {
			select {
			case _, ok := <-pipe:
				if !ok {
					return
				}
			}
		}
	}
}

// getErrorsFromPipe selects all received errors and returns them
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
