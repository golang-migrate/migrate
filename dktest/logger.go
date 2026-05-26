package dktest

// Logger is the interface used to log messages.
type Logger interface {
	Log(...interface{})
}
