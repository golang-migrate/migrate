package source

// Executor is the interface that wraps the Execute method.
type Executor interface {
	Execute(dbInstance interface{}) error
}

// The ExecutorFunc type is an adapter to allow the use of
// ordinary functions as Executor.
type ExecutorFunc func(interface{}) error

// Execute calls f(w, r).
func (f ExecutorFunc) Execute(dbInstance interface{}) error {
	return f(dbInstance)
}
