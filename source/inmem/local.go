package inmem

import (
	"fmt"
	"strings"
	"sync"

	"github.com/golang-migrate/migrate/v4/source"
)

const (
	schemeKey = "inmem"
	scheme    = schemeKey + "://" // construct inmem:// as scheme in Open function
)

// localMemory saves all *source.Migrations mapped by key
type localMemory struct {
	lock       sync.RWMutex
	data       map[string]*source.Migrations
	versionLog map[string]uint
}

// migrations is a local global variables to save any source.Migrations
var migrations = &localMemory{
	data:       make(map[string]*source.Migrations),
	versionLog: make(map[string]uint),
}

// RegisterMigrations add new migrations for given key.
// You can add migrations struct in go routine.
func RegisterMigrations(key string, mig ...Migration) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrEmptyKey
	}

	migrations.lock.Lock()
	defer migrations.lock.Unlock()

	// Create new instance on map if not exist
	if m, exist := migrations.data[key]; !exist || m == nil {
		migrations.data[key] = source.NewMigrations()
	}

	for _, m := range mig {
		if m == nil {
			return ErrNilMigration
		}

		if v, exist := migrations.versionLog[key]; exist && v == m.Version() {
			return ErrDuplicateVersion{
				key: key,
				ver: m.Version(),
			}
		}

		migrations.versionLog[key] = m.Version()
		migrations.data[key].Append(&source.Migration{
			Version:    m.Version(),
			Identifier: m.Up(),
			Direction:  source.Up,
			Raw:        fmt.Sprintf("%d.memory.up", m.Version()),
		})

		migrations.data[key].Append(&source.Migration{
			Version:    m.Version(),
			Identifier: m.Down(),
			Direction:  source.Down,
			Raw:        fmt.Sprintf("%d.memory.down", m.Version()),
		})
	}

	return nil
}
