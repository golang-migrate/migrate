package gomethods

import (
	//"bytes"
	"reflect"
	"fmt"
	"strings"
	"os"
	"path"
	"bufio"
	"github.com/dimag-jfrog/migrate/driver"
	"github.com/dimag-jfrog/migrate/file"
	"github.com/dimag-jfrog/migrate/migrate/direction"
)


type MissingMethodError string
func (e MissingMethodError) Error() string   { return "Non existing migrate method: " + string(e) }


type WrongMethodSignatureError string
func (e WrongMethodSignatureError) Error() string   { return fmt.Sprintf("Method %s has wrong signature", e) }

type MethodInvocationFailedError struct {
	MethodName string
	Err error
}

func (e *MethodInvocationFailedError) Error() string {
	return fmt.Sprintf("Method %s returned an error: %v", e.MethodName, e.Error)
}


type Migrator struct {
	Driver driver.DriverWithFilenameParser
	RollbackOnFailure bool
}

func (m *Migrator) Migrate(f file.File, pipe chan interface{}) error {
	methods, err := m.getMigrationMethods(f)
	if err != nil {
		pipe <- err
		return err
	}

	for i, methodName := range methods {
		pipe <- methodName
		err := m.Invoke(methodName)
		if err != nil {
			pipe <- err
			if !m.RollbackOnFailure {
				return err
			}

			// on failure, try to rollback methods in this migration
			for j := i-1; j >= 0; j-- {
				rollbackToMethodName := getRollbackToMethod(methods[j])
				pipe <- rollbackToMethodName
				err = m.Invoke(rollbackToMethodName)
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

func (m *Migrator) IsValid(methodName string) bool {
	return reflect.ValueOf(m.Driver).MethodByName(methodName).IsValid()
}

func (m *Migrator) Invoke(methodName string) error {
	name := methodName
	migrateMethod := reflect.ValueOf(m.Driver).MethodByName(name)
	if !migrateMethod.IsValid() {
		return MissingMethodError(methodName)
	}

	retValues := migrateMethod.Call([]reflect.Value{})
	if len(retValues) != 1 {
		return WrongMethodSignatureError(name)
	}

	if !retValues[0].IsNil() {
		err, ok := retValues[0].Interface().(error)
		if !ok {
			return WrongMethodSignatureError(name)
		}
		return &MethodInvocationFailedError{ MethodName:name, Err:err}
	}

	return nil
}

func reverseInPlace(a []string) {
	for i := 0; i < len(a)/2; i++ {
		j := len(a) - i - 1
		a[i], a[j] = a[j], a[i]
	}
}

func getRollbackToMethod(methodName string) string {
	if strings.HasSuffix(methodName, "_up") {
		return strings.TrimSuffix(methodName, "_up") + "_down"
	} else {
		return strings.TrimSuffix(methodName, "_down") + "_up"
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
		//n := bytes.IndexByte(file.Content, 0)
		//n := bytes.Index(file.Content, []byte{0})
		//s := string(file.Content[:n])
		s := string(file.Content)
		return strings.Split(s, "\n"), nil
	}
}

func (m *Migrator) getMigrationMethods(f file.File) ([]string, error) {
	var lines, methods []string
	lines, err := getFileLines(f)
	if err != nil {
		return nil, err
	}

	for _, line := range lines {
		operationName := strings.TrimSpace(line)

		if operationName == "" || strings.HasPrefix(operationName, "--") {
			// an empty line or a comment, ignore
			continue
		}

		upMethodName := operationName + "_up"
		downMethodName := operationName + "_down"

		if !m.IsValid(upMethodName) {
			return nil, MissingMethodError(upMethodName)
		}
		if !m.IsValid(downMethodName) {
			return nil, MissingMethodError(downMethodName)
		}

		if f.Direction == direction.Up {
			methods = append(methods, upMethodName)
		} else {
			methods = append(methods, downMethodName)
		}
	}

	_,_,fileType,_ := m.Driver.FilenameParser().Parse(f.FileName)
	if fileType == direction.Both && f.Direction == direction.Down {
		reverseInPlace(methods)
	}
	return methods, nil

}
