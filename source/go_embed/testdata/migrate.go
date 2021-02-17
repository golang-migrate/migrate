package testdata

import (
	"embed"
)

//go:embed *.sql
var Source embed.FS
