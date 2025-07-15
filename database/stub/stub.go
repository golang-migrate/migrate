package stub

import (
	"io"
	"reflect"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	database.Register("stub", &Stub{})
}

type Stub struct {
	Url               string
	Instance          interface{}
	CurrentVersion    int
	MigrationSequence []string
	LastRunMigration  []byte // todo: make []string
	IsDirty           bool
	isLocked          atomic.Bool

	Config *Config
}

func (s *Stub) Open(url string) (database.Driver, error) {
	return &Stub{
		Url:               url,
		CurrentVersion:    database.NilVersion,
		MigrationSequence: make([]string, 0),
		Config:            &Config{},
	}, nil
}

type Config struct {
	Triggers map[string]func(response interface{}) error
}

type TriggerResponse struct {
	Driver  *Stub
	Config  *Config
	Trigger string
	Detail  interface{}
}

func WithInstance(instance interface{}, config *Config) (database.Driver, error) {
	return &Stub{
		Instance:          instance,
		CurrentVersion:    database.NilVersion,
		MigrationSequence: make([]string, 0),
		Config:            config,
	}, nil
}

func (s *Stub) Close() error {
	return nil
}

func (s *Stub) AddTriggers(t map[string]func(response interface{}) error) {
	s.Config.Triggers = t
}

func (s *Stub) Trigger(name string, detail interface{}) error {
	if s.Config.Triggers == nil {
		return nil
	}

	if trigger, ok := s.Config.Triggers[name]; ok {
		return trigger(TriggerResponse{
			Driver:  s,
			Config:  s.Config,
			Trigger: name,
			Detail:  detail,
		})
	}

	return nil
}

func (s *Stub) Lock() error {
	if !s.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (s *Stub) Unlock() error {
	if !s.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (s *Stub) Run(migration io.Reader) error {
	m, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	if err := s.Trigger(database.TrigRunPre, struct {
		Query string
	}{Query: string(m[:])}); err != nil {
		return &database.Error{OrigErr: err, Err: "failed to trigger RunPre"}
	}

	s.LastRunMigration = m
	s.MigrationSequence = append(s.MigrationSequence, string(m[:]))

	if err := s.Trigger(database.TrigRunPost, struct {
		Query string
	}{Query: string(m[:])}); err != nil {
		return &database.Error{OrigErr: err, Err: "failed to trigger RunPost"}
	}

	return nil
}

func (s *Stub) SetVersion(version int, state bool) error {
	s.CurrentVersion = version
	s.IsDirty = state
	return nil
}

func (s *Stub) Version() (version int, dirty bool, err error) {
	return s.CurrentVersion, s.IsDirty, nil
}

const DROP = "DROP"

func (s *Stub) Drop() error {
	s.CurrentVersion = database.NilVersion
	s.LastRunMigration = nil
	s.MigrationSequence = append(s.MigrationSequence, DROP)
	return nil
}

func (s *Stub) EqualSequence(seq []string) bool {
	return reflect.DeepEqual(seq, s.MigrationSequence)
}
