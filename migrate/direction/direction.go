// Package direction just holds convenience constants for Up and Down migrations.
package direction

// Direction - type that indicates direction of migration(up or down)
type Direction int

const (
	// Up - up migration
	Up Direction = +1
	// Down - down migration
	Down Direction = -1
)
