package source

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
)

var ErrParse = fmt.Errorf("no match")

var (
	DefaultParse = Parse
	DefaultRegex = Regex
)

// Regex matches the following pattern:
//
//	123_name.up.ext
//	123_name.down.ext
var (
	Regex = regexp.MustCompile(
		`^([0-9]+)_(.*)\.(` + string(Down) + `|` + string(Up) + `)\.(.*)$`,
	)
	semVerRegex = regexp.MustCompile(
		`^(.*)\.(` + string(Down) + `|` + string(Up) + `)\.(.*)$`,
	)
)

// Parse returns Migration for matching Regex pattern.
func Parse(raw string) (*Migration, error) {
	m := Regex.FindStringSubmatch(raw)
	if len(m) == 5 {
		versionUint64, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, err
		}
		return &Migration{
			Version:    uint(versionUint64),
			Identifier: m[2],
			Direction:  Direction(m[3]),
			Raw:        raw,
		}, nil
	}
	return nil, ErrParse
}

// ParseSemVer returns Migration for matching migration file under semver dir.
func ParseSemVer(version uint64, semVerDir, migFile string) (*Migration, error) {
	m := semVerRegex.FindStringSubmatch(migFile)
	if len(m) == 4 {
		return &Migration{
			Version:    uint(version),
			Identifier: m[1] + "(" + semVerDir + ")",
			Direction:  Direction(m[2]),
			Raw:        filepath.Join(semVerDir, migFile),
		}, nil
	}
	return nil, ErrParse
}
