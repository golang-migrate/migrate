package database

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

const advisoryLockIdSalt uint = 1486364155

// GenerateAdvisoryLockId inspired by rails migrations, see https://goo.gl/8o9bCT
func GenerateAdvisoryLockId(databaseName string, additionalNames ...string) (string, error) {
	buf := bytes.NewBufferString(databaseName)
	for _, name := range additionalNames {
		buf.WriteByte(0)
		buf.WriteString(name)
	}
	sum := crc32.ChecksumIEEE(buf.Bytes())
	sum = sum * uint32(advisoryLockIdSalt)
	return fmt.Sprintf("%v", sum), nil
}
