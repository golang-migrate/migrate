package migrate

import (
	"bufio"
	"fmt"
	"io"
	"time"
)

var DefaultBufferSize = uint(100000)

type Migration struct {
	Identifier    string
	Version       uint
	TargetVersion int

	Body         io.ReadCloser
	BufferedBody io.Reader
	BufferSize   uint
	bufferWriter io.WriteCloser

	Scheduled         time.Time
	StartedBuffering  time.Time
	FinishedBuffering time.Time
	FinishedReading   time.Time
	BytesRead         int64
}

func NewMigration(body io.ReadCloser, identifier string, version uint, targetVersion int) (*Migration, error) {
	tnow := time.Now()
	m := &Migration{
		Identifier:    identifier,
		Version:       version,
		TargetVersion: targetVersion,
		Scheduled:     tnow,
	}

	if body == nil {
		if len(identifier) == 0 {
			m.Identifier = "<empty>"
		}

		m.StartedBuffering = tnow
		m.FinishedBuffering = tnow
		m.FinishedReading = tnow
		return m, nil
	}

	br, bw := io.Pipe()
	m.Body = body // want to simulate low latency? newSlowReader(body)
	m.BufferSize = DefaultBufferSize
	m.BufferedBody = br
	m.bufferWriter = bw
	return m, nil
}

func (m *Migration) String() string {
	return fmt.Sprintf("%v [%v=>%v]", m.Identifier, m.Version, m.TargetVersion)
}

func (m *Migration) StringLong() string {
	directionStr := "u"
	if m.TargetVersion < int(m.Version) {
		directionStr = "d"
	}
	return fmt.Sprintf("%v/%v %v", m.Version, directionStr, m.Identifier)
}

// Buffer buffers up to BufferSize (blocking, call with goroutine)
func (m *Migration) Buffer() error {
	if m.Body == nil {
		return nil
	}

	m.StartedBuffering = time.Now()

	b := bufio.NewReaderSize(m.Body, int(m.BufferSize))

	// start reading from body, peek won't move the read pointer though
	// poor man's solution?
	b.Peek(int(m.BufferSize))

	m.FinishedBuffering = time.Now()

	// write to bufferWriter, this will block until
	// something starts reading from m.Buffer
	n, err := b.WriteTo(m.bufferWriter)
	if err != nil {
		return err
	}

	m.FinishedReading = time.Now()
	m.BytesRead = n

	// close bufferWriter so Buffer knows that there is no
	// more data coming
	m.bufferWriter.Close()

	// it's safe to close the Body too
	m.Body.Close()

	return nil
}
