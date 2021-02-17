// +build go1.6

package testdata

import (
	"embed"
)

//go:embed *.sql
var Source embed.FS
