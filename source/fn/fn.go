package fn

import (
	"fmt"
	"io"
	"os"

	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("func", &Func{})
}

// / Migration represents a single migration.
type Migration struct {
	Up   source.Executor
	Down source.Executor
}

// Func is a source.Driver that reads migrations from a map of Migrations.
type Func struct {
	migrations *source.Migrations
}

// Open implements source.Driver.
func (b *Func) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// WithInstance returns a source.Driver that is backed by an map of Migrations.
func WithInstance(mgrs map[string]*Migration) (source.Driver, error) {
	gb := &Func{
		migrations: source.NewMigrations(),
	}

	executors := map[string]source.Executor{}
	for k, mgr := range mgrs {
		if mgr == nil {
			continue
		}

		if mgr.Up != nil {
			executors[k+"."+string(source.Up)+".func"] = mgr.Up
		}
		if mgr.Down != nil {
			executors[k+"."+string(source.Down)+".func"] = mgr.Down
		}
	}

	for k, exec := range executors {
		m, err := source.DefaultParse(k)
		if err != nil {
			continue // ignore keys that we can't parse
		}
		m.Executor = exec

		if !gb.migrations.Append(m) {
			return nil, fmt.Errorf("unable to parse key %v", k)
		}
	}

	return gb, nil
}

func (gb *Func) Close() error {
	return nil
}

func (gb *Func) First() (version uint, err error) {
	v, ok := gb.migrations.First()
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (gb *Func) Prev(version uint) (prevVersion uint, err error) {
	v, ok := gb.migrations.Prev(version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (gb *Func) Next(version uint) (nextVersion uint, err error) {
	v, ok := gb.migrations.Next(version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (gb *Func) ReadUp(version uint) (r io.ReadCloser, e source.Executor, identifier string, err error) {
	if m, ok := gb.migrations.Up(version); ok {
		return nil, m.Executor, m.Identifier, nil
	}
	return nil, nil, "", os.ErrNotExist
}

func (gb *Func) ReadDown(version uint) (r io.ReadCloser, e source.Executor, identifier string, err error) {
	if m, ok := gb.migrations.Down(version); ok {
		return nil, m.Executor, m.Identifier, nil
	}
	return nil, nil, "", os.ErrNotExist
}
