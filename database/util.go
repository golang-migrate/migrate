package database

import (
	"fmt"
	"hash/crc32"
	"strings"
)

const advisoryLockIdSalt uint = 1486364155

// GenerateAdvisoryLockId inspired by rails migrations, see https://goo.gl/8o9bCT
func GenerateAdvisoryLockId(databaseName string, additionalNames ...string) (string, error) {
	if len(additionalNames) > 0 {
		databaseName = strings.Join(append(additionalNames, databaseName), "\x00")
	}
	sum := crc32.ChecksumIEEE([]byte(databaseName))
	sum = sum * uint32(advisoryLockIdSalt)
	return fmt.Sprintf("%v", sum), nil
}
