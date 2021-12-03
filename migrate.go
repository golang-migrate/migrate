// Package migrate reads migrations from sources and runs them against databases.
// Sources are defined by the `source.Driver` and databases by the `database.Driver`
// interface. The driver interfaces are kept "dumb", all migration logic is kept
// in this package.
package migrate

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/golang-migrate/migrate/v4/database"
	iurl "github.com/golang-migrate/migrate/v4/internal/url"
	"github.com/golang-migrate/migrate/v4/source"
)

// DefaultPrefetchMigrations sets the number of migrations to pre-read
// from the source. This is helpful if the source is remote, but has little
// effect for a local source (i.e. file system).
// Please note that this setting has a major impact on the memory usage,
// since each pre-read migration is buffered in memory. See DefaultBufferSize.
var DefaultPrefetchMigrations = uint(10)

// DefaultLockTimeout sets the max time a database driver has to acquire a lock.
var DefaultLockTimeout = 15 * time.Second

var (
	ErrNoChange       = errors.New("no change")
	ErrNilVersion     = errors.New("no migration")
	ErrInvalidVersion = errors.New("version must be >= -1")
	ErrLocked         = errors.New("database locked")
	ErrLockTimeout    = errors.New("timeout: can't acquire database lock")
)

// ErrShortLimit is an error returned when not enough migrations
// can be returned by a source for a given limit.
type ErrShortLimit struct {
	Short uint
}

// Error implements the error interface.
func (e ErrShortLimit) Error() string {
	return fmt.Sprintf("limit %v short", e.Short)
}

type ErrDirty struct {
	Version int
}

func (e ErrDirty) Error() string {
	return fmt.Sprintf("Dirty database version %v. Fix and force version.", e.Version)
}

type Migrate struct {
	sourceName   string
	sourceDrv    source.Driver
	databaseName string
	databaseDrv  database.Driver

	// Log accepts a Logger interface
	Log Logger

	// GracefulStop accepts `true` and will stop executing migrations
	// as soon as possible at a safe break point, so that the database
	// is not corrupted.
	GracefulStop chan bool
	isLockedMu   *sync.Mutex

	isGracefulStop bool
	isLocked       bool

	// PrefetchMigrations defaults to DefaultPrefetchMigrations,
	// but can be set per Migrate instance.
	PrefetchMigrations uint

	// LockTimeout defaults to DefaultLockTimeout,
	// but can be set per Migrate instance.
	LockTimeout time.Duration
}

// New returns a new Migrate instance from a source URL and a database URL.
// The URL scheme is defined by each driver.
func New(sourceURL, databaseURL string) (*Migrate, error) {
	m := newCommon()

	sourceName, err := iurl.SchemeFromURL(sourceURL)
	if err != nil {
		return nil, err
	}
	m.sourceName = sourceName

	databaseName, err := iurl.SchemeFromURL(databaseURL)
	if err != nil {
		return nil, err
	}
	m.databaseName = databaseName

	sourceDrv, err := source.Open(sourceURL)
	if err != nil {
		return nil, err
	}
	m.sourceDrv = sourceDrv

	databaseDrv, err := database.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	m.databaseDrv = databaseDrv

	return m, nil
}

// NewWithDatabaseInstance returns a new Migrate instance from a source URL
// and an existing database instance. The source URL scheme is defined by each driver.
// Use any string that can serve as an identifier during logging as databaseName.
// You are responsible for closing the underlying database client if necessary.
func NewWithDatabaseInstance(sourceURL string, databaseName string, databaseInstance database.Driver) (*Migrate, error) {
	m := newCommon()

	sourceName, err := iurl.SchemeFromURL(sourceURL)
	if err != nil {
		return nil, err
	}
	m.sourceName = sourceName

	m.databaseName = databaseName

	sourceDrv, err := source.Open(sourceURL)
	if err != nil {
		return nil, err
	}
	m.sourceDrv = sourceDrv

	m.databaseDrv = databaseInstance

	return m, nil
}

// NewWithSourceInstance returns a new Migrate instance from an existing source instance
// and a database URL. The database URL scheme is defined by each driver.
// Use any string that can serve as an identifier during logging as sourceName.
// You are responsible for closing the underlying source client if necessary.
func NewWithSourceInstance(sourceName string, sourceInstance source.Driver, databaseURL string) (*Migrate, error) {
	m := newCommon()

	databaseName, err := iurl.SchemeFromURL(databaseURL)
	if err != nil {
		return nil, err
	}
	m.databaseName = databaseName

	m.sourceName = sourceName

	databaseDrv, err := database.Open(databaseURL)
	if err != nil {
		return nil, err
	}
	m.databaseDrv = databaseDrv

	m.sourceDrv = sourceInstance

	return m, nil
}

// NewWithInstance returns a new Migrate instance from an existing source and
// database instance. Use any string that can serve as an identifier during logging
// as sourceName and databaseName. You are responsible for closing down
// the underlying source and database client if necessary.
func NewWithInstance(sourceName string, sourceInstance source.Driver, databaseName string, databaseInstance database.Driver) (*Migrate, error) {
	m := newCommon()

	m.sourceName = sourceName
	m.databaseName = databaseName

	m.sourceDrv = sourceInstance
	m.databaseDrv = databaseInstance

	return m, nil
}

func newCommon() *Migrate {
	return &Migrate{
		GracefulStop:       make(chan bool, 1),
		PrefetchMigrations: DefaultPrefetchMigrations,
		LockTimeout:        DefaultLockTimeout,
		isLockedMu:         &sync.Mutex{},
	}
}

// Close closes the source and the database.
func (m *Migrate) Close() (source error, database error) {
	databaseSrvClose := make(chan error)
	sourceSrvClose := make(chan error)

	m.logVerbosePrintf("Closing source and database\n")

	go func() {
		databaseSrvClose <- m.databaseDrv.Close()
	}()

	go func() {
		sourceSrvClose <- m.sourceDrv.Close()
	}()

	return <-sourceSrvClose, <-databaseSrvClose
}

// Migrate looks at the currently active migration version,
// then migrates either up or down to the specified version.
func (m *Migrate) Migrate(version uint) error {
	if err := m.lock(); err != nil {
		return err
	}

	curVersion, err := m.versionClean()
	if err != nil {
		return m.unlockErr(err)
	}

	return m.unlockErr(m.runMigrations(func(h *providerHelper) error {
		return m.read(curVersion, int(version), h)
	}))
}

// Steps looks at the currently active migration version.
// It will migrate up if n > 0, and down if n < 0.
func (m *Migrate) Steps(n int) error {
	if n == 0 {
		return ErrNoChange
	}

	if err := m.lock(); err != nil {
		return err
	}

	curVersion, err := m.versionClean()
	if err != nil {
		return m.unlockErr(err)
	}

	return m.unlockErr(m.runMigrations(func(h *providerHelper) error {
		if n > 0 {
			return m.readUp(curVersion, n, h)
		}

		return m.readDown(curVersion, -n, h)
	}))
}

// Up looks at the currently active migration version
// and will migrate all the way up (applying all up migrations).
func (m *Migrate) Up() error {
	if err := m.lock(); err != nil {
		return err
	}

	curVersion, err := m.versionClean()
	if err != nil {
		return m.unlockErr(err)
	}

	return m.unlockErr(m.runMigrations(func(h *providerHelper) error {
		return m.readUp(curVersion, -1, h)
	}))
}

// Down looks at the currently active migration version
// and will migrate all the way down (applying all down migrations).
func (m *Migrate) Down() error {
	if err := m.lock(); err != nil {
		return err
	}

	curVersion, err := m.versionClean()
	if err != nil {
		return m.unlockErr(err)
	}

	return m.unlockErr(m.runMigrations(func(h *providerHelper) error {
		return m.readDown(curVersion, -1, h)
	}))
}

// Drop deletes everything in the database.
func (m *Migrate) Drop() error {
	if err := m.lock(); err != nil {
		return err
	}
	if err := m.databaseDrv.Drop(); err != nil {
		return m.unlockErr(err)
	}
	return m.unlock()
}

// Run runs any migration provided by you against the database.
// It does not check any currently active version in database.
// Usually you don't need this function at all. Use Migrate,
// Steps, Up or Down instead.
func (m *Migrate) Run(migration ...*Migration) error {
	if len(migration) == 0 {
		return ErrNoChange
	}

	if err := m.lock(); err != nil {
		return err
	}

	_, err := m.versionClean()
	if err != nil {
		return m.unlockErr(err)
	}

	return m.unlockErr(m.runMigrations(func(h *providerHelper) error {
		for _, migr := range migration {
			if m.PrefetchMigrations > 0 && migr.Body != nil {
				m.logVerbosePrintf("Start buffering %v\n", migr.LogString())
			} else {
				m.logVerbosePrintf("Scheduled %v\n", migr.LogString())
			}

			pushed := h.pushMigration(migr)
			if !pushed {
				return nil
			}
		}

		return nil
	}))
}

// Force sets a migration version.
// It does not check any currently active version in database.
// It resets the dirty state to false.
func (m *Migrate) Force(version int) error {
	if version < -1 {
		return ErrInvalidVersion
	}

	if err := m.lock(); err != nil {
		return err
	}

	if err := m.databaseDrv.SetVersion(version, false); err != nil {
		return m.unlockErr(err)
	}

	return m.unlock()
}

// Version returns the currently active migration version.
// If no migration has been applied, yet, it will return ErrNilVersion.
func (m *Migrate) Version() (version uint, dirty bool, err error) {
	v, d, err := m.databaseDrv.Version()
	if err != nil {
		return 0, false, err
	}

	if v == database.NilVersion {
		return 0, false, ErrNilVersion
	}

	return suint(v), d, nil
}

// versionClean returns the currently active migration version
// while making assertion that the active migration is clean (not dirty).
func (m *Migrate) versionClean() (version int, err error) {
	v, d, err := m.databaseDrv.Version()
	if err != nil {
		return 0, err
	}

	if d {
		return 0, ErrDirty{v}
	}

	return v, nil
}

// read reads either up or down migrations from source `from` to `to`.
// Each migration is then written to the ret channel.
func (m *Migrate) read(from int, to int, h *providerHelper) error {
	// check if from version exists
	if from >= 0 {
		if err := m.versionExists(suint(from)); err != nil {
			return err
		}
	}

	// check if to version exists
	if to >= 0 {
		if err := m.versionExists(suint(to)); err != nil {
			return err
		}
	}

	// no change?
	if from == to {
		return ErrNoChange
	}

	if from < to {
		// it's going up
		// apply first migration if from is nil version
		if from == -1 {
			firstVersion, err := m.sourceDrv.First()
			if err != nil {
				return err
			}

			migr, err := m.newMigration(firstVersion, int(firstVersion))
			if err != nil {
				return err
			}

			pushed := h.pushMigration(migr)
			if !pushed {
				return nil
			}

			from = int(firstVersion)
		}

		// run until we reach target ...
		for from < to {
			if m.stop() {
				return nil
			}

			next, err := m.sourceDrv.Next(suint(from))
			if err != nil {
				return err
			}

			migr, err := m.newMigration(next, int(next))
			if err != nil {
				return err
			}

			pushed := h.pushMigration(migr)
			if !pushed {
				return nil
			}

			from = int(next)
		}

	} else {
		// it's going down
		// run until we reach target ...
		for from > to && from >= 0 {
			if m.stop() {
				return nil
			}

			prev, err := m.sourceDrv.Prev(suint(from))
			if errors.Is(err, os.ErrNotExist) && to == -1 {
				// apply nil migration
				migr, err := m.newMigration(suint(from), -1)
				if err != nil {
					return err
				}

				pushed := h.pushMigration(migr)
				if !pushed {
					return nil
				}

				return nil

			} else if err != nil {
				return err
			}

			migr, err := m.newMigration(suint(from), int(prev))
			if err != nil {
				return err
			}

			pushed := h.pushMigration(migr)
			if !pushed {
				return nil
			}

			from = int(prev)
		}
	}

	return nil
}

// readUp reads up migrations from `from` limitted by `limit`.
// limit can be -1, implying no limit and reading until there are no more migrations.
// Each migration is then written to the ret channel.
func (m *Migrate) readUp(from int, limit int, h *providerHelper) error {
	// check if from version exists
	if from >= 0 {
		if err := m.versionExists(suint(from)); err != nil {
			return err
		}
	}

	if limit == 0 {
		return ErrNoChange
	}

	count := 0
	for count < limit || limit == -1 {
		if m.stop() {
			return nil
		}

		// apply first migration if from is nil version
		if from == -1 {
			firstVersion, err := m.sourceDrv.First()
			if err != nil {
				return err
			}

			migr, err := m.newMigration(firstVersion, int(firstVersion))
			if err != nil {
				return err
			}

			pushed := h.pushMigration(migr)
			if !pushed {
				return nil
			}

			from = int(firstVersion)
			count++
			continue
		}

		// apply next migration
		next, err := m.sourceDrv.Next(suint(from))
		if errors.Is(err, os.ErrNotExist) {
			// no limit, but no migrations applied?
			if limit == -1 && count == 0 {
				return ErrNoChange
			}

			// no limit, reached end
			if limit == -1 {
				return nil
			}

			// reached end, and didn't apply any migrations
			if limit > 0 && count == 0 {
				return os.ErrNotExist
			}

			// applied less migrations than limit?
			if count < limit {
				return ErrShortLimit{suint(limit - count)}
			}
		}
		if err != nil {
			return err
		}

		migr, err := m.newMigration(next, int(next))
		if err != nil {
			return err
		}

		pushed := h.pushMigration(migr)
		if !pushed {
			return nil
		}

		from = int(next)
		count++
	}

	return nil
}

// readDown reads down migrations from `from` limitted by `limit`.
// limit can be -1, implying no limit and reading until there are no more migrations.
// Each migration is then written to the ret channel.
func (m *Migrate) readDown(from int, limit int, h *providerHelper) error {
	// check if from version exists
	if from >= 0 {
		if err := m.versionExists(suint(from)); err != nil {
			return err
		}
	}

	if limit == 0 {
		return ErrNoChange
	}

	// no change if already at nil version
	if from == -1 && limit == -1 {
		return ErrNoChange
	}

	// can't go over limit if already at nil version
	if from == -1 && limit > 0 {
		return os.ErrNotExist
	}

	count := 0
	for count < limit || limit == -1 {
		if m.stop() {
			return nil
		}

		prev, err := m.sourceDrv.Prev(suint(from))
		if errors.Is(err, os.ErrNotExist) {
			// no limit or haven't reached limit, apply "first" migration
			if limit == -1 || limit-count > 0 {
				firstVersion, err := m.sourceDrv.First()
				if err != nil {
					return err
				}

				migr, err := m.newMigration(firstVersion, -1)
				if err != nil {
					return err
				}

				pushed := h.pushMigration(migr)
				if !pushed {
					return nil
				}

				count++
			}

			if count < limit {
				return ErrShortLimit{suint(limit - count)}
			}

			return nil
		}
		if err != nil {
			return err
		}

		migr, err := m.newMigration(suint(from), int(prev))
		if err != nil {
			return err
		}

		pushed := h.pushMigration(migr)
		if !pushed {
			return nil
		}

		from = int(prev)
		count++
	}

	return nil
}

// runMigrations reads *Migration from channel that background provider concurrently writes to.
// Each migration is then proxied to the database driver and run against the database.
// Before running a newly received migration it will check if it's supposed
// to stop execution because it might have received a stop signal on the
// GracefulStop channel.
func (m *Migrate) runMigrations(providerFn func(h *providerHelper) error) error {
	providerDone := make(chan error, 1)
	runnerDone := make(chan bool)

	// Channel of migrations for background provider
	// to write to and runner to read from.
	// Closed after provider finishes its job.
	ret := make(chan *Migration, m.PrefetchMigrations)

	// Start the background provider.
	go func() {
		h := providerHelper{
			m:    m,
			ch:   ret,
			exit: runnerDone,
		}

		providerDone <- providerFn(&h)
		close(ret)
	}()

	// Start the migration runner.
	err := func() error {
		defer close(runnerDone)

		for migr := range ret {
			if m.stop() {
				return nil
			}

			// set version with dirty state
			if err := m.databaseDrv.SetVersion(migr.TargetVersion, true); err != nil {
				return err
			}

			if migr.Body != nil {
				m.logVerbosePrintf("Read and execute %v\n", migr.LogString())
				if err := m.databaseDrv.Run(migr.BufferedBody); err != nil {
					return err
				}
			}

			// set clean state
			if err := m.databaseDrv.SetVersion(migr.TargetVersion, false); err != nil {
				return err
			}

			endTime := time.Now()
			readTime := migr.FinishedReading.Sub(migr.StartedBuffering)
			runTime := endTime.Sub(migr.FinishedReading)

			// log either verbose or normal
			if m.Log != nil {
				if m.Log.Verbose() {
					m.logPrintf("Finished %v (read %v, ran %v)\n", migr.LogString(), readTime, runTime)
				} else {
					m.logPrintf("%v (%v)\n", migr.LogString(), readTime+runTime)
				}
			}
		}

		return nil
	}()

	// Runner has finished.
	// Now wait for provider to finish.
	if perr := <-providerDone; perr != nil {
		return perr
	}

	return err
}

// versionExists checks the source if either the up or down migration for
// the specified migration version exists.
func (m *Migrate) versionExists(version uint) (result error) {
	// try up migration first
	up, _, err := m.sourceDrv.ReadUp(version)
	if err == nil {
		defer func() {
			if errClose := up.Close(); errClose != nil {
				result = multierror.Append(result, errClose)
			}
		}()
	}
	if errors.Is(err, os.ErrExist) {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// then try down migration
	down, _, err := m.sourceDrv.ReadDown(version)
	if err == nil {
		defer func() {
			if errClose := down.Close(); errClose != nil {
				result = multierror.Append(result, errClose)
			}
		}()
	}
	if errors.Is(err, os.ErrExist) {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	err = fmt.Errorf("no migration found for version %d: %w", version, err)
	m.logErr(err)
	return err
}

// stop returns true if no more migrations should be run against the database
// because a stop signal was received on the GracefulStop channel.
// Calls are cheap and this function is not blocking.
func (m *Migrate) stop() bool {
	if m.isGracefulStop {
		return true
	}

	select {
	case <-m.GracefulStop:
		m.isGracefulStop = true
		return true

	default:
		return false
	}
}

// newMigration is a helper func that returns a *Migration for the
// specified version and targetVersion.
func (m *Migrate) newMigration(version uint, targetVersion int) (*Migration, error) {
	var migr *Migration

	if targetVersion >= int(version) {
		r, identifier, err := m.sourceDrv.ReadUp(version)
		if errors.Is(err, os.ErrNotExist) {
			// create "empty" migration
			migr, err = NewMigration(nil, "", version, targetVersion)
			if err != nil {
				return nil, err
			}

		} else if err != nil {
			return nil, err

		} else {
			// create migration from up source
			migr, err = NewMigration(r, identifier, version, targetVersion)
			if err != nil {
				return nil, err
			}
		}

	} else {
		r, identifier, err := m.sourceDrv.ReadDown(version)
		if errors.Is(err, os.ErrNotExist) {
			// create "empty" migration
			migr, err = NewMigration(nil, "", version, targetVersion)
			if err != nil {
				return nil, err
			}

		} else if err != nil {
			return nil, err

		} else {
			// create migration from down source
			migr, err = NewMigration(r, identifier, version, targetVersion)
			if err != nil {
				return nil, err
			}
		}
	}

	if m.PrefetchMigrations > 0 && migr.Body != nil {
		m.logVerbosePrintf("Start buffering %v\n", migr.LogString())
	} else {
		m.logVerbosePrintf("Scheduled %v\n", migr.LogString())
	}

	return migr, nil
}

// lock is a thread safe helper function to lock the database.
// It should be called as late as possible when running migrations.
func (m *Migrate) lock() error {
	m.isLockedMu.Lock()
	defer m.isLockedMu.Unlock()

	if m.isLocked {
		return ErrLocked
	}

	// create done channel, used in the timeout goroutine
	done := make(chan bool, 1)
	defer func() {
		done <- true
	}()

	// use errchan to signal error back to this context
	errchan := make(chan error, 2)

	// start timeout goroutine
	timeout := time.After(m.LockTimeout)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-timeout:
				errchan <- ErrLockTimeout
				return
			}
		}
	}()

	// now try to acquire the lock
	go func() {
		if err := m.databaseDrv.Lock(); err != nil {
			errchan <- err
		} else {
			errchan <- nil
		}
	}()

	// wait until we either receive ErrLockTimeout or error from Lock operation
	err := <-errchan
	if err == nil {
		m.isLocked = true
	}
	return err
}

// unlock is a thread safe helper function to unlock the database.
// It should be called as early as possible when no more migrations are
// expected to be executed.
func (m *Migrate) unlock() error {
	m.isLockedMu.Lock()
	defer m.isLockedMu.Unlock()

	if err := m.databaseDrv.Unlock(); err != nil {
		// BUG: Can potentially create a deadlock. Add a timeout.
		return err
	}

	m.isLocked = false
	return nil
}

// unlockErr calls unlock and returns a combined error
// if a prevErr is not nil.
func (m *Migrate) unlockErr(prevErr error) error {
	if err := m.unlock(); err != nil {
		return multierror.Append(prevErr, err)
	}
	return prevErr
}

// logPrintf writes to m.Log if not nil
func (m *Migrate) logPrintf(format string, v ...interface{}) {
	if m.Log != nil {
		m.Log.Printf(format, v...)
	}
}

// logVerbosePrintf writes to m.Log if not nil. Use for verbose logging output.
func (m *Migrate) logVerbosePrintf(format string, v ...interface{}) {
	if m.Log != nil && m.Log.Verbose() {
		m.Log.Printf(format, v...)
	}
}

// logErr writes error to m.Log if not nil
func (m *Migrate) logErr(err error) {
	if m.Log != nil {
		m.Log.Printf("error: %v", err)
	}
}

// providerHelper represents a helper for migration provider
// that groups together the original *Migrate used for migrations,
// a channel to read/write migrations from and a channel,
// that signals that provider may exit.
type providerHelper struct {
	m    *Migrate
	ch   chan<- *Migration
	exit <-chan bool
}

// pushMigration pushes migration to provider channel for runner to read from.
// Returns true if push was successful.
func (h *providerHelper) pushMigration(migr *Migration) (pushed bool) {
	select {
	case <-h.exit:
		return false
	case h.ch <- migr:
		go func() {
			if err := migr.Buffer(); err != nil {
				h.m.logErr(err)
			}
		}()

		return true
	}
}
