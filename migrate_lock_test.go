package migrate

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	dStub "github.com/golang-migrate/migrate/v4/database/stub"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

// blockingLockDB is a database.Driver whose Lock calls can be held open
// by the test, so the behaviour of Migrate.lock when the lock timeout
// fires while a Lock call is still in flight can be observed.
type blockingLockDB struct {
	stub *dStub.Stub

	mu          sync.Mutex
	locked      bool
	lockCalls   int
	unlockCalls int
	// gate, when non-nil, blocks Lock until it is closed.
	gate chan struct{}
	// gateErr, when non-nil, is returned by a gated Lock once the gate opens.
	gateErr error
}

func newBlockingLockDB() *blockingLockDB {
	return &blockingLockDB{stub: &dStub.Stub{CurrentVersion: database.NilVersion}}
}

func (d *blockingLockDB) Open(string) (database.Driver, error) { return d, nil }
func (d *blockingLockDB) Close() error                         { return nil }

func (d *blockingLockDB) Lock() error {
	d.mu.Lock()
	d.lockCalls++
	gate := d.gate
	gateErr := d.gateErr
	d.mu.Unlock()

	if gate != nil {
		<-gate
		if gateErr != nil {
			return gateErr
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.locked {
		return database.ErrLocked
	}
	d.locked = true
	return nil
}

func (d *blockingLockDB) Unlock() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.locked {
		return database.ErrNotLocked
	}
	d.locked = false
	d.unlockCalls++
	return nil
}

func (d *blockingLockDB) Run(migration io.Reader) error { return d.stub.Run(migration) }
func (d *blockingLockDB) Drop() error                   { return d.stub.Drop() }
func (d *blockingLockDB) SetVersion(v int, dirty bool) error {
	return d.stub.SetVersion(v, dirty)
}
func (d *blockingLockDB) Version() (int, bool, error) { return d.stub.Version() }

func (d *blockingLockDB) state() (locked bool, lockCalls, unlockCalls int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.locked, d.lockCalls, d.unlockCalls
}

// setGate installs a fresh gate for the next Lock call. The returned
// channel releases the blocked Lock when closed.
func (d *blockingLockDB) setGate(err error) chan struct{} {
	gate := make(chan struct{})
	d.mu.Lock()
	defer d.mu.Unlock()
	d.gate = gate
	d.gateErr = err
	return gate
}

func (d *blockingLockDB) clearGate() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.gate = nil
	d.gateErr = nil
}

func newBlockingLockMigrate(t *testing.T, db *blockingLockDB) *Migrate {
	t.Helper()
	src, err := sStub.WithInstance(nil, &sStub.Config{})
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewWithInstance(srcDrvNameStub, src, dbDrvNameStub, db)
	if err != nil {
		t.Fatal(err)
	}
	m.LockTimeout = 50 * time.Millisecond
	return m
}

func waitForLockState(t *testing.T, db *blockingLockDB, wantLocked bool, wantUnlocks int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		locked, _, unlocks := db.state()
		if locked == wantLocked && unlocks == wantUnlocks {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	locked, lockCalls, unlocks := db.state()
	t.Fatalf("timed out waiting for driver state locked=%v unlocks=%d, got locked=%v lockCalls=%d unlocks=%d",
		wantLocked, wantUnlocks, locked, lockCalls, unlocks)
}

// A Lock call that only completes after LockTimeout must not leave the
// database locked forever: once the late Lock succeeds, Migrate has to
// release the lock it no longer knows about, and the instance must stay
// usable instead of reporting ErrLocked for every later operation.
func TestLockTimeoutLateSuccessIsReleased(t *testing.T) {
	db := newBlockingLockDB()
	m := newBlockingLockMigrate(t, db)

	gate := db.setGate(nil)
	if err := m.Force(1); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("expected ErrLockTimeout, got %v", err)
	}

	// The timed-out attempt is still in flight: a second operation must
	// not start a competing Lock call.
	if err := m.Force(1); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked while timed-out lock is pending, got %v", err)
	}
	if _, lockCalls, _ := db.state(); lockCalls != 1 {
		t.Fatalf("expected exactly 1 Lock call while pending, got %d", lockCalls)
	}

	// Let the original Lock call succeed, long after the timeout.
	db.clearGate()
	close(gate)

	// The orphaned lock must be released again by Migrate itself.
	waitForLockState(t, db, false, 1)

	// And the instance must be usable again.
	if err := m.Force(7); err != nil {
		t.Fatalf("expected Force to succeed after orphan cleanup, got %v", err)
	}
	if v, dirty, err := m.Version(); err != nil || dirty || v != 7 {
		t.Fatalf("expected version 7 clean, got %v dirty=%v err=%v", v, dirty, err)
	}
}

// If the late Lock call fails instead of succeeding, there is nothing to
// release, but the pending state must still be cleared so the instance
// can be used again.
func TestLockTimeoutLateErrorClearsPending(t *testing.T) {
	db := newBlockingLockDB()
	m := newBlockingLockMigrate(t, db)

	errLateLock := errors.New("late lock failure")
	gate := db.setGate(errLateLock)
	if err := m.Force(1); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("expected ErrLockTimeout, got %v", err)
	}

	db.clearGate()
	close(gate)

	// No lock was acquired, so no Unlock may happen; poll until a new
	// operation is accepted again (it would return ErrLocked while the
	// timed-out attempt is still marked pending).
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := m.Force(3)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("expected ErrLocked while pending or nil after cleanup, got %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pending lock state to clear")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The only Unlock so far may come from the successful Force above;
	// the failed late Lock itself must not have produced one, and no
	// lock may be left behind.
	if locked, _, unlocks := db.state(); locked || unlocks != 1 {
		t.Fatalf("expected unlocked driver with 1 unlock (from the successful Force), got locked=%v unlocks=%d", locked, unlocks)
	}
}

// A Lock call that fails before the timeout behaves as before: the error
// is returned unchanged and the next operation can lock again.
func TestLockImmediateError(t *testing.T) {
	db := newBlockingLockDB()
	m := newBlockingLockMigrate(t, db)

	errLock := errors.New("lock failure")
	gate := db.setGate(errLock)
	close(gate) // gate already open: Lock returns errLock immediately

	if err := m.Force(1); !errors.Is(err, errLock) {
		t.Fatalf("expected %v, got %v", errLock, err)
	}
	db.clearGate()
	if err := m.Force(2); err != nil {
		t.Fatalf("expected Force to succeed after immediate lock error, got %v", err)
	}
}
