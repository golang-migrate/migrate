package bindata

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"

	"github.com/mattes/migrate/source"
)

type AssetFunc func(name string) ([]byte, error)

func Resource(names []string, afn AssetFunc) *AssetSource {
	return &AssetSource{
		Names:     names,
		AssetFunc: afn,
	}
}

type AssetSource struct {
	Names     []string
	AssetFunc AssetFunc
}

func init() {
	source.Register("go-bindata", &Bindata{})
}

// filename example: `123_name.up.ext`
// filename example: `123_name.down.ext`
var filenameRegex = regexp.MustCompile(`^([0-9]+)_(.*)\.(` + string(down) + `|` + string(up) + `)\.(.*)$`)

type Bindata struct {
	path        string
	filesIndex  uintSlice
	files       map[uint]map[direction]file
	assetSource *AssetSource
}

func (b *Bindata) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not yet implemented")
}

var (
	ErrNoAssetSource = fmt.Errorf("expects *AssetSource")
)

func WithInstance(instance interface{}) (source.Driver, error) {
	if _, ok := instance.(*AssetSource); !ok {
		return nil, ErrNoAssetSource
	}
	as := instance.(*AssetSource)

	bn := &Bindata{
		path:        "<go-bindata>",
		assetSource: as,
	}

	// parse file names and create internal data structure
	bn.files = make(map[uint]map[direction]file)
	for _, fi := range as.Names {
		pf, err := parseFilename(fi)
		if err != nil {
			continue // ignore files that we can't parse
		}

		if bn.files[pf.version] == nil {
			bn.files[pf.version] = make(map[direction]file)
		}

		// reject duplicate versions
		if dupf, dup := bn.files[pf.version][pf.direction]; dup {
			return nil, fmt.Errorf("duplicate file: %v and %v", dupf.filename, fi)
		}

		bn.files[pf.version][pf.direction] = *pf
	}

	// create index and sort
	bn.filesIndex = make(uintSlice, 0)
	for version, _ := range bn.files {
		bn.filesIndex = append(bn.filesIndex, version)
	}
	sort.Sort(bn.filesIndex)

	return bn, nil
}

func (b *Bindata) Close() error {
	return nil
}

func (b *Bindata) First() (version uint, err error) {
	if len(b.filesIndex) == 0 {
		return 0, &os.PathError{"first", b.path, os.ErrNotExist}
	}
	return b.filesIndex[0], nil
}

func (b *Bindata) Prev(version uint) (prevVersion uint, err error) {
	pos := b.findPos(version)
	if pos >= 1 && len(b.filesIndex) > pos-1 {
		return b.filesIndex[pos-1], nil
	}
	return 0, &os.PathError{fmt.Sprintf("prev for version %v", version), b.path, os.ErrNotExist}
}

func (b *Bindata) Next(version uint) (nextVersion uint, err error) {
	pos := b.findPos(version)
	if pos >= 0 && len(b.filesIndex) > pos+1 {
		return b.filesIndex[pos+1], nil
	}
	return 0, &os.PathError{fmt.Sprintf("next for version %v", version), b.path, os.ErrNotExist}
}

func (b *Bindata) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if _, ok := b.files[version]; ok {
		if upFile, ok := b.files[version][up]; ok {
			body, err := b.assetSource.AssetFunc(upFile.filename)
			if err != nil {
				return nil, "", err
			}
			return ioutil.NopCloser(bytes.NewReader(body)), upFile.name, nil
		}
	}
	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), b.path, os.ErrNotExist}
}

func (b *Bindata) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if _, ok := b.files[version]; ok {
		if downFile, ok := b.files[version][down]; ok {
			body, err := b.assetSource.AssetFunc(downFile.filename)
			if err != nil {
				return nil, "", err
			}
			return ioutil.NopCloser(bytes.NewReader(body)), downFile.name, nil
		}
	}
	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), b.path, os.ErrNotExist}
}

// findPos finds the position of a file in the index
// returns -1 if the version can't be found
func (b *Bindata) findPos(version uint) int {
	if len(b.filesIndex) > 0 {
		for i, v := range b.filesIndex {
			if v == version {
				return i
			}
		}
	}
	return -1
}

// file contains parsed filename details
type file struct {
	version   uint
	name      string
	direction direction
	extension string
	filename  string
}

var errParseFilenameNoMatch = fmt.Errorf("no match")

func parseFilename(filename string) (*file, error) {
	m := filenameRegex.FindStringSubmatch(filename)
	if len(m) == 5 {
		versionUint64, err := strconv.ParseUint(m[1], 10, 32)
		if err != nil {
			return nil, err
		}
		return &file{
			version:   uint(versionUint64),
			name:      m[2],
			direction: direction(m[3]),
			extension: m[4],
			filename:  filename,
		}, nil
	}
	return nil, errParseFilenameNoMatch
}

type direction string

const (
	down direction = "down"
	up             = "up"
)

type uintSlice []uint

func (s uintSlice) Len() int {
	return len(s)
}

func (s uintSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s uintSlice) Less(i, j int) bool {
	return s[i] < s[j]
}
