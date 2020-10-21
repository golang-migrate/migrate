package inmem

import (
	"io/ioutil"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/golang-migrate/migrate/v4/source"
)

const (
	testKey = "key"
)

type testCaseKeyEmptiness struct {
	key string
	err error
}

var testCasesKey = []testCaseKeyEmptiness{
	{
		key: "",
		err: ErrEmptyKey,
	},
	{
		key: " ",
		err: ErrEmptyKey,
	},
	{
		key: "   ", // more spaces
		err: ErrEmptyKey,
	},
	{
		key: "	", // tabs
		err: ErrEmptyKey,
	},
}

type mockMigration struct {
	Ver       uint
	UpQuery   string
	DownQuery string
}

func (m mockMigration) Version() uint { return m.Ver }

func (m mockMigration) Up() string { return m.UpQuery }

func (m mockMigration) Down() string { return m.DownQuery }

var _ Migration = (*mockMigration)(nil)

var firstMigration = &mockMigration{
	Ver:       1,
	UpQuery:   "1.up",
	DownQuery: "1.down",
}

var secondMigration = mockMigration{
	Ver:       2,
	UpQuery:   "2.up",
	DownQuery: "2.down",
}

// testMigrations only contains 2 migration versions
// this is a mock data
var testMigrations = []Migration{firstMigration, secondMigration}

var clear = func() {
	migrations = &localMemory{
		data:       make(map[string]*source.Migrations),
		versionLog: make(map[string]uint),
	}
}

func TestWithInstance_ErrNilMigration(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	driver, err := WithInstance(nil)
	assert.Equal(t, nil, driver)
	assert.Equal(t, ErrNilMigration, err)
}

func TestWithInstance(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	driver, err := WithInstance(testMigrations...)
	assert.NotEqual(t, nil, driver)
	assert.Equal(t, nil, err)
}

func TestMemory_Open_ErrEmptyUrl(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m := new(Memory)
	driver, err := m.Open("")
	assert.Equal(t, nil, driver)
	assert.Equal(t, ErrEmptyUrl, err)
}

func TestMemory_Open_ErrInvalidUrlScheme(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m := new(Memory)
	driver, err := m.Open("invalidScheme://key")
	assert.Equal(t, nil, driver)
	assert.Equal(t, ErrInvalidUrlScheme, err)
}

func TestMemory_Open_ErrEmptyKey(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m := new(Memory)
	for _, caseData := range testCasesKey {
		driver, err := m.Open(scheme + caseData.key)
		assert.Equal(t, nil, driver)
		assert.Equal(t, caseData.err, err)
	}

}

func TestMemory_Open_ErrNilMigration(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m := new(Memory)
	driver, err := m.Open(scheme + "notExist")
	assert.Equal(t, nil, driver)
	assert.Equal(t, ErrNilMigration, err)
}

func TestMemory_Close(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m := new(Memory)
	err := m.Close()
	assert.Equal(t, nil, err)
}

func TestMemory_First(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance(testMigrations...)
	assert.Equal(t, nil, err)

	v, err := m.First()
	assert.Equal(t, firstMigration.Version(), v)
	assert.Equal(t, nil, err)
}

func TestMemory_First_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance()
	assert.Equal(t, nil, err)

	v, err := m.First()
	assert.Equal(t, uint(0), v)
	assert.NotEqual(t, nil, err)
}

func TestMemory_Prev(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance(testMigrations...)
	assert.Equal(t, nil, err)

	v, err := m.Prev(secondMigration.Version())
	assert.Equal(t, firstMigration.Version(), v)
	assert.Equal(t, nil, err)
}

func TestMemory_Prev_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance()
	assert.Equal(t, nil, err)

	v, err := m.Prev(secondMigration.Version())
	assert.Equal(t, uint(0), v)
	assert.NotEqual(t, nil, err)
}

func TestMemory_Next(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance(testMigrations...)
	assert.Equal(t, nil, err)

	v, err := m.Next(firstMigration.Version())
	assert.Equal(t, secondMigration.Version(), v)
	assert.Equal(t, nil, err)
}

func TestMemory_Next_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance()
	assert.Equal(t, nil, err)

	v, err := m.Next(firstMigration.Version())
	assert.Equal(t, uint(0), v)
	assert.NotEqual(t, nil, err)
}

func TestMemory_ReadUp(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance(testMigrations...)
	assert.Equal(t, nil, err)

	reader, identifier, err := m.ReadUp(firstMigration.Version())
	assert.NotEqual(t, "", identifier)
	assert.Equal(t, nil, err)

	exp, err := ioutil.ReadAll(reader)
	assert.Equal(t, nil, err)
	assert.Equal(t, string(exp), firstMigration.Up())

}

func TestMemory_ReadUp_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance()
	assert.Equal(t, nil, err)

	reader, identifier, err := m.ReadUp(firstMigration.Version())
	assert.Equal(t, nil, reader)
	assert.Equal(t, "", identifier)
	assert.NotEqual(t, nil, err)
}

func TestMemory_ReadDown(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance(testMigrations...)
	assert.Equal(t, nil, err)

	reader, identifier, err := m.ReadDown(firstMigration.Version())
	assert.NotEqual(t, "", identifier)
	assert.Equal(t, nil, err)

	exp, err := ioutil.ReadAll(reader)
	assert.Equal(t, nil, err)
	assert.Equal(t, string(exp), firstMigration.Down())

}

func TestMemory_ReadDown_Error(t *testing.T) {
	t.Cleanup(func() {
		clear()
	})

	m, err := WithInstance()
	assert.Equal(t, nil, err)

	reader, identifier, err := m.ReadDown(firstMigration.Version())
	assert.Equal(t, nil, reader)
	assert.Equal(t, "", identifier)
	assert.NotEqual(t, nil, err)
}