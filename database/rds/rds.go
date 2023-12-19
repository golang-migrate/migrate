package rds

import (
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/krotscheck/go-rds-driver"
	"io"
	"strconv"
	"time"
)

var _ database.Driver = (*RDS)(nil) // explicit compile time type check

var (
	ErrInvalidDriver       = fmt.Errorf("invalid driver specified")
	ErrDirectDriverUse     = fmt.Errorf("this driver may not be invoked directly, please use Open or WithInstance")
	ErrInvalidConnector    = fmt.Errorf("invalid connector provided by driver")
	ErrUnrecognizedBackend = fmt.Errorf("the RDS backend was not recognized")
)

func init() {
	// Register the drivers
	database.Register(rds.DRIVERNAME, &RDS{})
}

// RDS uses the data API to detect which backend is represented by the DB ARN, and wraps the appropriate driver.
type RDS struct {
}

// Open the database driver given the provided DSN
func (r *RDS) Open(url string) (database.Driver, error) {
	// Construct a configuration instance from the DSN
	conf, err := rds.NewConfigFromDSN(url)
	if err != nil {
		return nil, err
	}

	// Go ahead and create an instance of the database.
	db, err := sql.Open(rds.DRIVERNAME, url)
	if err != nil {
		return nil, err
	}

	// Wrap the MySQL or Postgres driver around this database
	return WithInstance(db, conf)
}

// Close the underlying driver
func (r *RDS) Close() error {
	return ErrDirectDriverUse
}

// Lock the database
func (r *RDS) Lock() error {
	return ErrDirectDriverUse
}

// Unlock the database
func (r *RDS) Unlock() error {
	return ErrDirectDriverUse
}

// Run a migration
func (r *RDS) Run(migration io.Reader) error {
	return ErrDirectDriverUse
}

// SetVersion of the migration state
func (r *RDS) SetVersion(version int, dirty bool) error {
	return ErrDirectDriverUse
}

// Version of the database
func (r *RDS) Version() (version int, dirty bool, err error) {
	return 0, false, ErrDirectDriverUse
}

// Drop the database
func (r *RDS) Drop() error {
	return ErrDirectDriverUse
}

// WithInstance wraps a migration driver around an already created sql.DB instance.
func WithInstance(db *sql.DB, conf *rds.Config) (database.Driver, error) {
	// Pull the driver, make sure it's the right type
	d, ok := db.Driver().(*rds.Driver)
	if !ok {
		return nil, ErrInvalidDriver
	}

	// Grab ourselves a connector so we can figure out which type of RDS database we're talking to
	connector, err := d.OpenConnector(conf.ToDSN())
	if err != nil {
		return nil, err
	}

	// Cast the connector...
	rdsConnector, ok := connector.(*rds.Connector)
	if !ok {
		return nil, ErrInvalidConnector
	}

	// Wakeup the database. Note that this may fail based on the RDS provisioning details, they can take a while
	// to spin back up.
	dialect, err := rdsConnector.Wakeup()
	if err != nil {
		return nil, err
	}

	if _, ok := dialect.(*rds.DialectMySQL); ok {
		// If we detect the MySQL dialect, wrap the existing MySQL driver, but use our database instance.
		driverConf, err := mySQLConfig(conf)
		if err != nil {
			return nil, err
		}
		return mysql.WithInstance(db, driverConf)
	}
	if _, ok := dialect.(*rds.DialectPostgres); ok {
		// If we detect the Postgres dialect, wrap the existing postgres driver, but use our database instance.
		driverConf, err := postgresConfig(conf)
		if err != nil {
			return nil, err
		}
		return postgres.WithInstance(db, driverConf)
	}
	return nil, ErrUnrecognizedBackend
}

// mySQLConfig specific configuration extracted from a standard config instance
func mySQLConfig(c *rds.Config) (conf *mysql.Config, err error) {
	conf = &mysql.Config{
		DatabaseName: c.Database,
	}

	if values, ok := c.Custom["x-migrations-table"]; ok && len(values) > 0 {
		conf.MigrationsTable = values[0]
	}
	if values, ok := c.Custom["x-no-lock"]; ok && len(values) > 0 {
		conf.NoLock, err = strconv.ParseBool(values[0])
	}
	return
}

// postgresConfig specific configuration extracted from the custom parameters
func postgresConfig(c *rds.Config) (conf *postgres.Config, err error) {
	conf = &postgres.Config{
		DatabaseName: c.Database,
	}

	if values, ok := c.Custom["x-migrations-table"]; ok && len(values) > 0 {
		conf.MigrationsTable = values[0]
	}
	if values, ok := c.Custom["x-migrations-table-quoted"]; ok && len(values) > 0 {
		conf.MigrationsTableQuoted, err = strconv.ParseBool(values[0])
	}
	if values, ok := c.Custom["x-multi-statement"]; ok && len(values) > 0 {
		conf.MultiStatementEnabled, err = strconv.ParseBool(values[0])
	}
	if values, ok := c.Custom["x-statement-timeout"]; ok && len(values) > 0 {
		conf.StatementTimeout, err = time.ParseDuration(values[0])
	}
	if values, ok := c.Custom["x-multi-statement-max-size"]; ok && len(values) > 0 {
		conf.MultiStatementMaxSize, err = strconv.Atoi(values[0])
	}

	return
}
