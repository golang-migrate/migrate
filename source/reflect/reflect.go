package reflect

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("reflect", &reflectSource{})
}

type reflectSource struct {
	target any
	labels []string
	up     []string
	down   []string
}

func New(target any) (source.Driver, error) {
	driver := &reflectSource{target: target}
	return driver.Open("")
}

func (r *reflectSource) Open(url string) (source.Driver, error) {
	if r.target == nil {
		return nil, fmt.Errorf("no target. source must be created with reflect.New()")
	}

	// already opened
	if len(r.labels) > 0 {
		return r, nil
	}

	// get the fields, these will always be in the same order as defined
	t := reflect.TypeOf(r.target).Elem()

	// the next step is to match the up clauses and down clauses
	// keep track of them in a map so they can be found later
	stubs := map[string]int{}

	r.up = make([]string, t.NumField()+1)
	r.down = make([]string, t.NumField()+1)
	r.labels = make([]string, t.NumField()+1)
	version := 1
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		last := strings.LastIndexByte(f.Name, '_')
		if last < 0 {
			return nil, fmt.Errorf("struct field must end with '_up' or '_down': %s", f.Name)
		}

		tag := f.Tag.Get("migrate")
		if tag != "" {
			v, err := strconv.Atoi(tag)
			if err != nil {
				return nil, fmt.Errorf("invalid tag, expected number: %s", tag)
			}
			version = v
		}

		switch f.Name[last:] {
		case "_up":
			r.up[version] = f.Name
			prefix := f.Name[:last]
			stubs[prefix] = version
			r.labels[version] = prefix
			version++
		case "_down":
			prefix := f.Name[:last]
			if ix, ok := stubs[prefix]; ok {
				r.down[ix] = f.Name
			} else {
				r.down[version] = f.Name
				stubs[prefix] = version
				r.labels[version] = prefix
				version++
			}
		default:
			return nil, fmt.Errorf("struct field must end with '_up' or '_down': %s", f.Name)
		}
	}

	return r, nil
}

func (r *reflectSource) Close() error {
	// no-op
	return nil
}

func (r *reflectSource) First() (version uint, err error) {
	return 1, nil
}

func (r *reflectSource) Prev(version uint) (prevVersion uint, err error) {
	v := int(version)
	if v < 1 || v >= len(r.up) {
		return 0, os.ErrNotExist
	}
	if r.up[v] == "" && r.down[v] == "" {
		return 0, os.ErrNotExist
	}
	v--
	for v > 0 {
		if r.up[v] != "" {
			return uint(v), nil
		}
		if r.down[v] != "" {
			return uint(v), nil
		}
		v--
	}
	return 0, os.ErrNotExist
}

func (r *reflectSource) Next(version uint) (nextVersion uint, err error) {
	v := int(version)
	if v < 1 || v >= len(r.up) {
		return 0, os.ErrNotExist
	}
	if r.up[v] == "" && r.down[v] == "" {
		return 0, os.ErrNotExist
	}
	v++
	for v < len(r.up) {
		if r.up[v] != "" {
			return uint(v), nil
		}
		if r.down[v] != "" {
			return uint(v), nil
		}
		v++
	}
	return 0, os.ErrNotExist
}

func (r *reflectSource) ReadUp(version uint) (io.ReadCloser, string, error) {
	ix := int(version)
	if r.up[ix] == "" {
		return nil, r.labels[ix], os.ErrNotExist
	}
	val := reflect.ValueOf(r.target).Elem()
	field := val.FieldByName(r.up[ix])
	return io.NopCloser(bytes.NewBufferString(field.String())), r.labels[ix], nil
}

func (r *reflectSource) ReadDown(version uint) (io.ReadCloser, string, error) {
	ix := int(version)
	if r.down[ix] == "" {
		return nil, r.labels[ix], os.ErrNotExist
	}
	val := reflect.ValueOf(r.target).Elem()
	field := val.FieldByName(r.down[ix])
	return io.NopCloser(bytes.NewBufferString(field.String())), r.labels[ix], nil
}
