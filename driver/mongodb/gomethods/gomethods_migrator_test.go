package gomethods

import (
	"reflect"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"

	pipep "github.com/mattes/migrate/pipe"
)

type FakeGoMethodsInvoker struct {
	InvokedMethods []string
}

func (invoker *FakeGoMethodsInvoker) Validate(methodName string) error {
	if methodName == "V001_some_non_existing_method_up" {
		return MethodNotFoundError(methodName)
	}

	return nil
}

func (invoker *FakeGoMethodsInvoker) Invoke(methodName string) error {
	invoker.InvokedMethods = append(invoker.InvokedMethods, methodName)

	if methodName == "V001_some_failing_method_up" || methodName == "V001_some_failing_method_down" {
		return &MethodInvocationFailedError{
			MethodName: methodName,
			Err:        SomeError{},
		}
	}
	return nil
}

type SomeError struct{}

func (e SomeError) Error() string { return "Some error happened" }

func TestMigrate(t *testing.T) {
	cases := []struct {
		name                   string
		file                   file.File
		expectedInvokedMethods []string
		expectedErrors         []error
		expectRollback         bool
	}{
		{
			name: "up migration invokes up methods",
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.up.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations_up
						V001_init_users_up
					`),
			},
			expectedInvokedMethods: []string{"V001_init_organizations_up", "V001_init_users_up"},
			expectedErrors:         []error{},
		},
		{
			name: "down migration invoked down methods",
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.down.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_users_down
						V001_init_organizations_down
					`),
			},
			expectedInvokedMethods: []string{"V001_init_users_down", "V001_init_organizations_down"},
			expectedErrors:         []error{},
		},
		{
			name: "up migration: non-existing method causes migration not to execute",
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.up.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations_up
						V001_init_users_up
						V001_some_non_existing_method_up
					`),
			},
			expectedInvokedMethods: []string{},
			expectedErrors:         []error{MethodNotFoundError("V001_some_non_existing_method_up")},
		},
		{
			name: "up migration: failing method stops execution",
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.up.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations_up
						V001_some_failing_method_up
						V001_init_users_up
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_organizations_up",
				"V001_some_failing_method_up",
			},
			expectedErrors: []error{&MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_up",
				Err:        SomeError{},
			}},
		},
		{
			name: "down migration: failing method stops migration",
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.down.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_users_down
						V001_some_failing_method_down
						V001_init_organizations_down
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_users_down",
				"V001_some_failing_method_down",
			},
			expectedErrors: []error{&MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_down",
				Err:        SomeError{},
			}},
		},
		{
			name:           "up migration: failing method causes rollback in rollback mode",
			expectRollback: true,
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.up.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Up,
				Content: []byte(`
						V001_init_organizations_up
						V001_init_users_up
						V001_some_failing_method_up
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_organizations_up",
				"V001_init_users_up",
				"V001_some_failing_method_up",
				"V001_init_users_down",
				"V001_init_organizations_down",
			},
			expectedErrors: []error{&MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_up",
				Err:        SomeError{},
			}},
		},
		{
			name:           "down migration: failing method causes rollback in rollback mode",
			expectRollback: true,
			file: file.File{
				Path:      "/foobar",
				FileName:  "001_foobar.down.gm",
				Version:   1,
				Name:      "foobar",
				Direction: direction.Down,
				Content: []byte(`
						V001_init_users_down
						V001_some_failing_method_down
						V001_init_organizations_down
					`),
			},
			expectedInvokedMethods: []string{
				"V001_init_users_down",
				"V001_some_failing_method_down",
				"V001_init_users_up",
			},
			expectedErrors: []error{&MethodInvocationFailedError{
				MethodName: "V001_some_failing_method_down",
				Err:        SomeError{},
			}},
		},
	}

	for _, c := range cases {
		migrator := Migrator{}
		fakeInvoker := &FakeGoMethodsInvoker{InvokedMethods: []string{}}

		migrator.MethodInvoker = fakeInvoker
		migrator.RollbackOnFailure = c.expectRollback

		pipe := pipep.New()
		go func() {
			migrator.Migrate(c.file, pipe)
			close(pipe)
		}()
		errs := pipep.ReadErrors(pipe)

		var failed bool
		if !reflect.DeepEqual(fakeInvoker.InvokedMethods, c.expectedInvokedMethods) {
			failed = true
			t.Errorf("case '%s': FAILED\nexpected invoked methods %v\nbut got %v", c.name, c.expectedInvokedMethods, fakeInvoker.InvokedMethods)
		}
		if !reflect.DeepEqual(errs, c.expectedErrors) {
			failed = true
			t.Errorf("case '%s': FAILED\nexpected errors %v\nbut got %v", c.name, c.expectedErrors, errs)

		}
		if !failed {
			//t.Logf("case '%s': PASSED", c.name)
		}
	}
}

func TestGetRollbackToMethod(t *testing.T) {
	cases := []struct {
		method                 string
		expectedRollbackMethod string
	}{
		{"some_method_up", "some_method_down"},
		{"some_method_down", "some_method_up"},
		{"up_down_up", "up_down_down"},
		{"down_up", "down_down"},
		{"down_down", "down_up"},
		{"some_method", ""},
	}

	for _, c := range cases {
		actualRollbackMethod := getRollbackToMethod(c.method)
		if actualRollbackMethod != c.expectedRollbackMethod {
			t.Errorf("Expected rollback method to be %s but got %s", c.expectedRollbackMethod, actualRollbackMethod)
		}
	}
}
