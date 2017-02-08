package migrate

type Logger interface {
	Printf(format string, v ...interface{})
	Verbose() bool
}
