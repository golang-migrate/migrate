package source

import (
	"sort"
)

type Direction string

const (
	Down Direction = "down"
	Up             = "up"
)

type Migration struct {
	Version    uint
	Identifier string
	Direction  Direction
	Raw        string
}

type Migrations struct {
	index      uintSlice
	migrations map[uint]map[Direction]*Migration
}

func NewMigrations() *Migrations {
	return &Migrations{
		index:      make(uintSlice, 0),
		migrations: make(map[uint]map[Direction]*Migration),
	}
}

func (i *Migrations) Append(m *Migration) (ok bool) {
	if m == nil {
		return false
	}

	if i.migrations[m.Version] == nil {
		i.migrations[m.Version] = make(map[Direction]*Migration)
	}

	// reject duplicate versions
	if _, dup := i.migrations[m.Version][m.Direction]; dup {
		return false
	}

	i.migrations[m.Version][m.Direction] = m
	i.buildIndex()

	return true
}

func (i *Migrations) buildIndex() {
	i.index = make(uintSlice, 0)
	for version, _ := range i.migrations {
		i.index = append(i.index, version)
	}
	sort.Sort(i.index)
}

func (i *Migrations) First() (version uint, ok bool) {
	if len(i.index) == 0 {
		return 0, false
	}
	return i.index[0], true
}

func (i *Migrations) Prev(version uint) (prevVersion uint, ok bool) {
	pos := i.findPos(version)
	if pos >= 1 && len(i.index) > pos-1 {
		return i.index[pos-1], true
	}
	return 0, false
}

func (i *Migrations) Next(version uint) (nextVersion uint, ok bool) {
	pos := i.findPos(version)
	if pos >= 0 && len(i.index) > pos+1 {
		return i.index[pos+1], true
	}
	return 0, false
}

func (i *Migrations) Up(version uint) (m *Migration, ok bool) {
	if _, ok := i.migrations[version]; ok {
		if mx, ok := i.migrations[version][Up]; ok {
			return mx, true
		}
	}
	return nil, false
}

func (i *Migrations) Down(version uint) (m *Migration, ok bool) {
	if _, ok := i.migrations[version]; ok {
		if mx, ok := i.migrations[version][Down]; ok {
			return mx, true
		}
	}
	return nil, false
}

func (i *Migrations) findPos(version uint) int {
	if len(i.index) > 0 {
		for i, v := range i.index {
			if v == version {
				return i
			}
		}
	}
	return -1
}

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
