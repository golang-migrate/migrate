package gomethods

import (
	"reflect"
	"testing"

	"github.com/dimag-jfrog/migrate/file"
	"github.com/dimag-jfrog/migrate/migrate/direction"

	pipep "github.com/dimag-jfrog/migrate/pipe"
)

type FakeGoMethodsDriver struct {
	InvokedMethods []string
	Migrator Migrator
}

func (driver *FakeGoMethodsDriver) Initialize(url string) error {
	return nil
}

func (driver *FakeGoMethodsDriver) Close() error {
	return nil
}

func (driver *FakeGoMethodsDriver) FilenameParser() file.FilenameParser {
	return file.UpDownAndBothFilenameParser{ FilenameExtension: driver.FilenameExtension() }
}

func (driver *FakeGoMethodsDriver) FilenameExtension() string {
	return "gm"
}

func (driver *FakeGoMethodsDriver) Version() (uint64, error) {
	return uint64(0), nil
}

func (driver *FakeGoMethodsDriver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f
	return
}

func (driver *FakeGoMethodsDriver) V001_init_organizations_up() error {
	driver.InvokedMethods = append(driver.InvokedMethods, "V001_init_organizations_up")
	return nil
}

func (driver *FakeGoMethodsDriver) V001_init_organizations_down() error {
	driver.InvokedMethods = append(driver.InvokedMethods, "V001_init_organizations_down")
	return nil

}

func (driver *FakeGoMethodsDriver) V001_init_users_up() error {
	driver.InvokedMethods = append(driver.InvokedMethods, "V001_init_users_up")
	return nil
}

func (driver *FakeGoMethodsDriver) V001_init_users_down() error {
	driver.InvokedMethods = append(driver.InvokedMethods, "V001_init_users_down")
	return nil
}

type SomeError struct{}
func (e SomeError) Error() string   { return "Some error happened" }

func (driver *FakeGoMethodsDriver) V001_some_failing_method_up() error {
	driver.InvokedMethods = append(driver.InvokedMethods, "V001_some_failing_method_up")
	return SomeError{}
}

func (driver *FakeGoMethodsDriver) V001_some_failing_method_down() error {
	driver.InvokedMethods = append(driver.InvokedMethods, "V001_some_failing_method_down")
	return SomeError{}
}

func TestMigrate(t *testing.T) {
	cases := []struct {
		name string
		file file.File
		expectedInvokedMethods []string
		expectedErrors []error
		expectRollback bool
	}{
		{
			name: "up migration, both directions-file: invokes up methods in order",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{"V001_init_organizations_up", "V001_init_users_up"},
			expectedErrors: []error{},
		},
		{
			name: "down migration, both-directions-file: reverts direction of invoked down methods",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_organizations
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{"V001_init_users_down", "V001_init_organizations_down"},
			expectedErrors: []error{},
		},
		{
			name: "up migration, up direction-file: invokes up methods in order",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.up.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{"V001_init_organizations_up", "V001_init_users_up"},
			expectedErrors: []error{},
		},
		{
			name: "down migration, down directions-file: keeps order of invoked down methods",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.down.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_organizations
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{"V001_init_organizations_down", "V001_init_users_down"},
			expectedErrors: []error{},
		},
		{
			name: "up migration: non-existing method causes migration not to execute",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations
						V001_init_users
						V001_some_non_existing_method
					`),
			},
			expectedInvokedMethods: []string{},
			expectedErrors: []error{ MissingMethodError("V001_some_non_existing_method_up") },
		},
		{
			name: "up migration: failing method stops execution",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations
						V001_some_failing_method
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_organizations_up",
				"V001_some_failing_method_up",
			},
			expectedErrors: []error{ &MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_up",
				Err: SomeError{},
			}},
		},
		{
			name: "down migration, both-directions-file: failing method stops migration",
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_organizations
						V001_some_failing_method
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_users_down",
				"V001_some_failing_method_down",
			},
			expectedErrors: []error{ &MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_down",
				Err: SomeError{},
			}},
		},
		{
			name: "up migration: failing method causes rollback in rollback mode",
			expectRollback: true,
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations
						V001_init_users
						V001_some_failing_method
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_organizations_up",
				"V001_init_users_up",
				"V001_some_failing_method_up",
				"V001_init_users_down",
				"V001_init_organizations_down",
			},
			expectedErrors: []error{ &MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_up",
				Err: SomeError{},
			}},
		},
		{
			name: "down migration, both-directions-file: failing method causes rollback in rollback mode",
			expectRollback: true,
			file: file.File {
				Path:      "/foobar",
				FileName:  "001_foobar.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_organizations
						V001_some_failing_method
						V001_init_users
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_users_down",
				"V001_some_failing_method_down",
				"V001_init_users_up",
			},
			expectedErrors: []error{ &MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_down",
				Err: SomeError{},
			}},
		},

	}

	for _, c := range cases {
		migrator := Migrator{}
		d := &FakeGoMethodsDriver{Migrator: migrator, InvokedMethods:[]string{}}
		migrator.Driver = d
		migrator.RollbackOnFailure = c.expectRollback

		pipe := pipep.New()
		go func() {
			migrator.Migrate(c.file, pipe)
			close(pipe)
		}()
		errs := pipep.ReadErrors(pipe)

		var failed bool
		if !reflect.DeepEqual(d.InvokedMethods, c.expectedInvokedMethods) {
			failed = true
			t.Errorf("case '%s': FAILED\nexpected invoked methods %v\nbut got %v", c.name, c.expectedInvokedMethods, d.InvokedMethods)
		}
		if !reflect.DeepEqual(errs, c.expectedErrors) {
			failed = true
			t.Errorf("case '%s': FAILED\nexpected errors %v\nbut got %v", c.name, c.expectedErrors, errs)

		}
		if !failed {
			t.Logf("case '%s': PASSED", c.name)
		}
	}
}



func TestGetRollbackToMethod(t *testing.T) {
	cases := []struct {
		method string
		expectedRollbackMethod string
	}{
		{"some_method_up", "some_method_down"},
		{"some_method_down", "some_method_up"},
		{"up_down_up", "up_down_down"},
		{"down_up", "down_down"},
		{"down_down", "down_up"},
	}

	for _, c := range cases {
		actualRollbackMethod := getRollbackToMethod(c.method)
		if actualRollbackMethod != c.expectedRollbackMethod {
			t.Errorf("Expected rollback method to be %s but got %s", c.expectedRollbackMethod, actualRollbackMethod)
		}
	}
}

func TestReverseInPlace(t *testing.T) {
	methods := []string {
		"method1_down",
		"method2_down",
		"method3_down",
		"method4_down",
		"method5_down",
	}

	expectedReversedMethods := []string {
		"method5_down",
		"method4_down",
		"method3_down",
		"method2_down",
		"method1_down",
	}

	reverseInPlace(methods)

	if !reflect.DeepEqual(methods, expectedReversedMethods) {
		t.Errorf("Expected reverse methods %v but got %v", expectedReversedMethods, methods)
	}
}

