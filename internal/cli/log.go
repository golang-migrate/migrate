package cli

import (
	"fmt"
	logpkg "log"
	"os"
)

// Log represents the logger
type Log struct {
	verbose bool
}

// Printf prints out formatted string into a log
func (l *Log) Printf(format string, v ...any) {
	if l.verbose {
		logpkg.Printf(format, v...)
	} else {
		fmt.Fprintf(os.Stderr, format, v...)
	}
}

// Println prints out args into a log
func (l *Log) Println(args ...any) {
	if l.verbose {
		logpkg.Println(args...)
	} else {
		fmt.Fprintln(os.Stderr, args...)
	}
}

// Verbose shows if verbose print enabled
func (l *Log) Verbose() bool {
	return l.verbose
}

func (l *Log) fatal(args ...any) {
	l.Println(args...)
	os.Exit(1)
}

func (l *Log) fatalErr(err error) {
	l.fatal("error:", err)
}
