package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/lib/pq"
)

// Ensure Postgres implements MigrationStorageDriver
var _ database.MigrationStorageDriver = &Postgres{}

// ensureEnhancedVersionTable checks if the enhanced versions table exists and creates/updates it.
// This version includes columns for storing migration scripts.
func (p *Postgres) ensureEnhancedVersionTable() (err error) {
	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("unlock error: %v, original error: %v", e, err)
			}
		}
	}()

	exists, err := p.tableExists()
	if err != nil {
		return err
	}

	if !exists {
		return p.createEnhancedTable()
	}

	return p.addMissingColumns()
}

// tableExists checks if the migrations table exists
func (p *Postgres) tableExists() (bool, error) {
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
	row := p.conn.QueryRowContext(context.Background(), query, p.config.migrationsSchemaName, p.config.migrationsTableName)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return false, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return count > 0, nil
}

// createEnhancedTable creates the migrations table with all required columns
func (p *Postgres) createEnhancedTable() error {
	query := `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (
		version bigint not null primary key, 
		dirty boolean not null,
		up_script text,
		down_script text,
		created_at timestamp with time zone default now()
	)`
	if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

// addMissingColumns adds any missing columns to existing table
func (p *Postgres) addMissingColumns() error {
	columns := []string{"up_script", "down_script", "created_at"}
	
	for _, column := range columns {
		exists, err := p.columnExists(column)
		if err != nil {
			return err
		}
		
		if !exists {
			if err := p.addColumn(column); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// columnExists checks if a specific column exists in the migrations table
func (p *Postgres) columnExists(columnName string) (bool, error) {
	query := `SELECT COUNT(1) FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`
	
	var count int
	err := p.conn.QueryRowContext(context.Background(), query, 
		p.config.migrationsSchemaName, p.config.migrationsTableName, columnName).Scan(&count)
	if err != nil {
		return false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	
	return count > 0, nil
}

// addColumn adds a specific column to the migrations table
func (p *Postgres) addColumn(columnName string) error {
	var columnDef string
	switch columnName {
	case "up_script", "down_script":
		columnDef = "text"
	case "created_at":
		columnDef = "timestamp with time zone default now()"
	default:
		return fmt.Errorf("unknown column: %s", columnName)
	}
	
	alterQuery := `ALTER TABLE ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + 
		pq.QuoteIdentifier(p.config.migrationsTableName) + ` ADD COLUMN ` + columnName + ` ` + columnDef
	if _, err := p.conn.ExecContext(context.Background(), alterQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(alterQuery)}
	}
	return nil
}

// StoreMigration stores the up and down migration scripts for a given version
func (p *Postgres) StoreMigration(version uint, upScript, downScript []byte) error {
	// Ensure the enhanced table exists
	if err := p.ensureEnhancedVersionTable(); err != nil {
		return err
	}

	query := `INSERT INTO ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + 
		pq.QuoteIdentifier(p.config.migrationsTableName) + 
		` (version, dirty, up_script, down_script) VALUES ($1, false, $2, $3) 
		ON CONFLICT (version) DO UPDATE SET 
		up_script = EXCLUDED.up_script, 
		down_script = EXCLUDED.down_script,
		created_at = now()`

	_, err := p.conn.ExecContext(context.Background(), query, int64(version), string(upScript), string(downScript))
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// GetMigration retrieves the stored migration scripts for a given version
func (p *Postgres) GetMigration(version uint) (upScript, downScript []byte, err error) {
	query := `SELECT up_script, down_script FROM ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + 
		pq.QuoteIdentifier(p.config.migrationsTableName) + ` WHERE version = $1`

	var upScriptStr, downScriptStr sql.NullString
	err = p.conn.QueryRowContext(context.Background(), query, int64(version)).Scan(&upScriptStr, &downScriptStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, fmt.Errorf("migration version %d not found", version)
		}
		return nil, nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if upScriptStr.Valid {
		upScript = []byte(upScriptStr.String)
	}
	if downScriptStr.Valid {
		downScript = []byte(downScriptStr.String)
	}

	return upScript, downScript, nil
}

// GetStoredMigrations returns all migration versions that have scripts stored
func (p *Postgres) GetStoredMigrations() ([]uint, error) {
	query := `SELECT version FROM ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + 
		pq.QuoteIdentifier(p.config.migrationsTableName) + 
		` WHERE up_script IS NOT NULL OR down_script IS NOT NULL ORDER BY version ASC`

	rows, err := p.conn.QueryContext(context.Background(), query)
	if err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer rows.Close()

	var versions []uint
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, uint(version))
	}

	return versions, rows.Err()
}

// SyncMigrations ensures all available migrations up to maxVersion are stored in the database
func (p *Postgres) SyncMigrations(sourceDriver interface{}, maxVersion uint) error {
	srcDriver, ok := sourceDriver.(source.Driver)
	if !ok {
		return fmt.Errorf("source driver must implement source.Driver interface")
	}

	versions, err := p.collectVersions(srcDriver, maxVersion)
	if err != nil {
		return err
	}

	return p.storeMigrations(srcDriver, versions)
}

// collectVersions gets all migration versions up to maxVersion
func (p *Postgres) collectVersions(srcDriver source.Driver, maxVersion uint) ([]uint, error) {
	first, err := srcDriver.First()
	if err != nil {
		return nil, fmt.Errorf("failed to get first migration: %w", err)
	}

	var versions []uint
	currentVersion := first

	for currentVersion <= maxVersion {
		versions = append(versions, currentVersion)

		next, err := srcDriver.Next(currentVersion)
		if err != nil {
			if err.Error() == "file does not exist" { // Handle os.ErrNotExist
				break
			}
			return nil, fmt.Errorf("failed to get next migration after %d: %w", currentVersion, err)
		}
		currentVersion = next
	}

	return versions, nil
}

// storeMigrations reads and stores migration scripts for the given versions
func (p *Postgres) storeMigrations(srcDriver source.Driver, versions []uint) error {
	for _, version := range versions {
		upScript, err := p.readMigrationScript(srcDriver, version, true)
		if err != nil {
			return err
		}

		downScript, err := p.readMigrationScript(srcDriver, version, false)
		if err != nil {
			return err
		}

		// Store the migration if we have at least one script
		if len(upScript) > 0 || len(downScript) > 0 {
			if err := p.StoreMigration(version, upScript, downScript); err != nil {
				return fmt.Errorf("failed to store migration %d: %w", version, err)
			}
		}
	}

	return nil
}

// readMigrationScript reads a migration script (up or down) for a given version
func (p *Postgres) readMigrationScript(srcDriver source.Driver, version uint, isUp bool) ([]byte, error) {
	var reader io.ReadCloser
	var err error

	if isUp {
		reader, _, err = srcDriver.ReadUp(version)
	} else {
		reader, _, err = srcDriver.ReadDown(version)
	}

	if err != nil {
		// It's OK if migration doesn't exist
		return nil, nil
	}

	defer reader.Close()
	script, err := io.ReadAll(reader)
	if err != nil {
		direction := "up"
		if !isUp {
			direction = "down"
		}
		return nil, fmt.Errorf("failed to read %s migration %d: %w", direction, version, err)
	}

	return script, nil
}
