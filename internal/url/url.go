package url

import (
	"errors"
	"fmt"
	"strings"
)

var errNoScheme = errors.New("no scheme")
var errEmptyURL = errors.New("URL cannot be empty")

// schemeFromURL returns the scheme from a URL string
func SchemeFromURL(url string) (string, error) {
	if url == "" {
		return "", errorWithURL(url, errEmptyURL)
	}

	i := strings.Index(url, ":")

	// No : or : is the first character.
	if i < 1 {
		return "", errorWithURL(url, errNoScheme)
	}

	return url[0:i], nil
}

// errorWithURL creates a new error including the provided url and wrapping the error it caused
func errorWithURL(url string, err error) error {
	return fmt.Errorf("invalid URL '%s': %w", url, err)
}
