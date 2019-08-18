package database

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
)

const advisoryLockIDSalt uint = 1486364155

// GenerateAdvisoryLockId inspired by rails migrations, see https://goo.gl/8o9bCT
func GenerateAdvisoryLockId(databaseName string, additionalNames ...string) (string, error) { // nolint: golint
	if len(additionalNames) > 0 {
		databaseName = strings.Join(append(additionalNames, databaseName), "\x00")
	}
	sum := crc32.ChecksumIEEE([]byte(databaseName))
	sum = sum * uint32(advisoryLockIDSalt)
	return fmt.Sprint(sum), nil
}

var errNoScheme = errors.New("no scheme")
var errEmptyURL = errors.New("URL cannot be empty")

// schemeFromURL returns the scheme from a URL string
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
