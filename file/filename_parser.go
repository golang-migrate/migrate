package file

import (
	"errors"
	"github.com/dimag-jfrog/migrate/migrate/direction"
	"regexp"
	"fmt"
	"strconv"
	"strings"
)


type FilenameParser interface {
	Parse (filename string) (version uint64, name string, d direction.Direction, err error)
}


var defaultFilenameRegexTemplate = `^([0-9]+)_(.*)\.(up|down)\.%s$`

func parseDefaultFilenameSchema(filename, filenameRegex string) (version uint64, name string, d direction.Direction, err error) {
	regexp := regexp.MustCompile(filenameRegex)
	matches := regexp.FindStringSubmatch(filename)
	if len(matches) != 4 {
		return 0, "", 0, errors.New("Unable to parse filename schema")
	}

	version, err = strconv.ParseUint(matches[1], 10, 0)
	if err != nil {
		return 0, "", 0, errors.New(fmt.Sprintf("Unable to parse version '%v' in filename schema", matches[0]))
	}

	name = matches[2]

	if matches[3] == "up" {
		d = direction.Up
	} else if matches[3] == "down" {
		d = direction.Down
	} else {
		return 0, "", 0, errors.New(fmt.Sprintf("Unable to parse up|down '%v' in filename schema", matches[3]))
	}

	return version, name, d, nil
}

type DefaultFilenameParser struct {
	FilenameExtension string
}

func (parser DefaultFilenameParser) Parse (filename string) (version uint64, name string, d direction.Direction, err error) {
	filenameRegex := fmt.Sprintf(defaultFilenameRegexTemplate, parser.FilenameExtension)
	return parseDefaultFilenameSchema(filename, filenameRegex)
}


type UpDownAndBothFilenameParser struct {
	FilenameExtension string
}

func (parser UpDownAndBothFilenameParser) Parse(filename string) (version uint64, name string, d direction.Direction, err error) {
	ext := parser.FilenameExtension
	if !strings.HasSuffix(filename, ext) {
		return 0, "", 0, errors.New("Filename ")
	}

	var matches []string
	if strings.HasSuffix(filename, ".up." + ext) || strings.HasSuffix(filename, ".down." + ext) {
		filenameRegex := fmt.Sprintf(defaultFilenameRegexTemplate, parser.FilenameExtension)
		return parseDefaultFilenameSchema(filename, filenameRegex)
	}

	regex := regexp.MustCompile(fmt.Sprintf(`^([0-9]+)_(.*)\.%s$`, ext))
	matches = regex.FindStringSubmatch(filename)
	if len(matches) != 3 {
		return 0, "", 0, errors.New("Unable to parse filename schema")
	}

	version, err = strconv.ParseUint(matches[1], 10, 0)
	if err != nil {
		return 0, "", 0, errors.New(fmt.Sprintf("Unable to parse version '%v' in filename schema", matches[0]))
	}
	name = matches[2]
	d = direction.Both

	return version, name, d, nil
}
