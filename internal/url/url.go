package url

import (
	"errors"
	"strings"
)

var errNoScheme = errors.New("no scheme")
var errEmptyURL = errors.New("URL cannot be empty")

// SchemeFromURL find scheme from beginning of string to the first colon
func SchemeFromURL(url string) (string, error) {
	if url == "" {
		return "", errEmptyURL
	}

	i := strings.Index(url, ":")

	// No : or : is the first character.
	if i < 1 {
		return "", errNoScheme
	}

	return url[0:i], nil
}
