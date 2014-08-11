package searchpath

import (
	"errors"
	"io/ioutil"
	"regexp"
)

var searchpath []string

func SetSearchPath(paths ...string) {
	searchpath = paths
}

func AppendSearchPath(path string) {
	searchpath = append(searchpath, path)
}

func PrependSearchPath(path string) {
	searchpath = append((searchpath)[:0], append([]string{path}, (searchpath)[0:]...)...)
}

func GetSearchPath() []string {
	return searchpath
}

// FindPath scans files in the search paths and
// returns the path where the regex matches at least twice
func FindPath(regex *regexp.Regexp) (path string, err error) {
	count := 0
	for _, path := range searchpath {
		// TODO refactor ioutil.ReadDir to read only first files per dir
		files, err := ioutil.ReadDir(path)
		if err == nil {
			for _, file := range files {
				if regex.MatchString(file.Name()) {
					count += 1
				}
				if count >= 2 {
					return path, nil
				}
			}
		}
	}
	return "", errors.New("no path found")
}
