package gomethods

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
)

type MethodNotFoundError string

func (e MethodNotFoundError) Error() string {
	return fmt.Sprintf("Method '%s' was not found. It is either not existing or has not been exported (starts with lowercase).", string(e))
}

type WrongMethodSignatureError string

func (e WrongMethodSignatureError) Error() string {
	return fmt.Sprintf("Method '%s' has wrong signature", string(e))
}

type MethodInvocationFailedError struct {
	MethodName string
	Err        error
}

func (e *MethodInvocationFailedError) Error() string {
	return fmt.Sprintf("Method '%s' returned an error: %v", e.MethodName, e.Err)
}

type MigrationMethodInvoker interface {
	Validate(methodName string) error
	Invoke(methodName string) error
}

type GoMethodsDriver interface {
	driver.Driver

	MigrationMethodInvoker
	MethodsReceiver() interface{}
	SetMethodsReceiver(r interface{}) error
}

type Migrator struct {
	RollbackOnFailure bool
	MethodInvoker     MigrationMethodInvoker
}

func (m *Migrator) Migrate(f file.File, pipe chan interface{}) error {
	methods, err := m.getMigrationMethods(f)
	if err != nil {
		pipe <- err
		return err
	}

	for i, methodName := range methods {
		pipe <- methodName
		err := m.MethodInvoker.Invoke(methodName)
		if err != nil {
			pipe <- err
			if !m.RollbackOnFailure {
				return err
			}

			// on failure, try to rollback methods in this migration
			for j := i - 1; j >= 0; j-- {
				rollbackToMethodName := getRollbackToMethod(methods[j])
				if rollbackToMethodName == "" {
					continue
				}
				if err := m.MethodInvoker.Validate(rollbackToMethodName); err != nil {
					continue
				}

				pipe <- rollbackToMethodName
				err = m.MethodInvoker.Invoke(rollbackToMethodName)
				if err != nil {
					pipe <- err
					break
				}
			}
			return err
		}
	}

	return nil
}

func getRollbackToMethod(methodName string) string {
	if strings.HasSuffix(methodName, "_up") {
		return strings.TrimSuffix(methodName, "_up") + "_down"
	} else if strings.HasSuffix(methodName, "_down") {
		return strings.TrimSuffix(methodName, "_down") + "_up"
	} else {
		return ""
	}
}

func getFileLines(file file.File) ([]string, error) {
	if len(file.Content) == 0 {
		lines := make([]string, 0)
		file, err := os.Open(path.Join(file.Path, file.FileName))
		if err != nil {
			return nil, err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		return lines, nil
	} else {
		s := string(file.Content)
		return strings.Split(s, "\n"), nil
	}
}

func (m *Migrator) getMigrationMethods(f file.File) (methods []string, err error) {
	var lines []string

	lines, err = getFileLines(f)
	if err != nil {
		return nil, err
	}

	for _, line := range lines {
		line := strings.TrimSpace(line)

		if line == "" || strings.HasPrefix(line, "--") {
			// an empty line or a comment, ignore
			continue
		}

		methodName := line
		if err := m.MethodInvoker.Validate(methodName); err != nil {
			return nil, err
		}

		methods = append(methods, methodName)
	}

	return methods, nil

}
