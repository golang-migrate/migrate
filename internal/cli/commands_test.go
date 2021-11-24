package cli

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type CreateCmdSuite struct {
	suite.Suite
}

func TestCreateCmdSuite(t *testing.T) {
	suite.Run(t, &CreateCmdSuite{})
}

func (s *CreateCmdSuite) mustCreateTempDir() string {
	tmpDir, err := ioutil.TempDir("", "migrate_")

	if err != nil {
		s.FailNow(err.Error())
	}

	return tmpDir
}

func (s *CreateCmdSuite) mustCreateDir(dir string) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		s.FailNow(err.Error())
	}
}

func (s *CreateCmdSuite) mustRemoveDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		s.FailNow(err.Error())
	}
}

func (s *CreateCmdSuite) mustWriteFile(dir, file, body string) {
	if err := ioutil.WriteFile(filepath.Join(dir, file), []byte(body), 0644); err != nil {
		s.FailNow(err.Error())
	}
}

func (s *CreateCmdSuite) mustGetwd() string {
	cwd, err := os.Getwd()

	if err != nil {
		s.FailNow(err.Error())
	}

	return cwd
}

func (s *CreateCmdSuite) mustChdir(dir string) {
	if err := os.Chdir(dir); err != nil {
		s.FailNow(err.Error())
	}
}

func (s *CreateCmdSuite) assertEmptyDir(dir string) bool {
	fis, err := ioutil.ReadDir(dir)

	if err != nil {
		return s.Fail(err.Error())
	}

	return s.Empty(fis)
}

func (s *CreateCmdSuite) TestNextSeqVersion() {
	cases := []struct {
		tid         string
		matches     []string
		seqDigits   int
		expected    string
		expectedErr error
	}{
		{"Bad digits", []string{}, 0, "", errInvalidSequenceWidth},
		{"Single digit initialize", []string{}, 1, "1", nil},
		{"Single digit malformed", []string{"bad"}, 1, "", errors.New("Malformed migration filename: bad")},
		{"Single digit no int", []string{"bad_bad"}, 1, "", errors.New(`strconv.ParseUint: parsing "bad": invalid syntax`)},
		{"Single digit negative seq", []string{"-5_test"}, 1, "", errors.New(`strconv.ParseUint: parsing "-5": invalid syntax`)},
		{"Single digit increment", []string{"3_test", "4_test"}, 1, "5", nil},
		{"Single digit overflow", []string{"9_test"}, 1, "", errors.New("Next sequence number 10 too large. At most 1 digits are allowed")},
		{"Zero-pad initialize", []string{}, 6, "000001", nil},
		{"Zero-pad malformed", []string{"bad"}, 6, "", errors.New("Malformed migration filename: bad")},
		{"Zero-pad no int", []string{"bad_bad"}, 6, "", errors.New(`strconv.ParseUint: parsing "bad": invalid syntax`)},
		{"Zero-pad negative seq", []string{"-000005_test"}, 6, "", errors.New(`strconv.ParseUint: parsing "-000005": invalid syntax`)},
		{"Zero-pad increment", []string{"000003_test", "000004_test"}, 6, "000005", nil},
		{"Zero-pad overflow", []string{"999999_test"}, 6, "", errors.New("Next sequence number 1000000 too large. At most 6 digits are allowed")},
		{"dir absolute path", []string{"/migrationDir/000001_test"}, 6, "000002", nil},
		{"dir relative path", []string{"migrationDir/000001_test"}, 6, "000002", nil},
		{"dir dot prefix", []string{"./migrationDir/000001_test"}, 6, "000002", nil},
		{"dir parent prefix", []string{"../migrationDir/000001_test"}, 6, "000002", nil},
		{"dir no prefix", []string{"000001_test"}, 6, "000002", nil},
	}

	for _, c := range cases {
		s.Run(c.tid, func() {
			v, err := nextSeqVersion(c.matches, c.seqDigits)

			if c.expectedErr != nil {
				s.EqualError(err, c.expectedErr.Error())
			} else {
				s.NoError(err)
				s.Equal(c.expected, v)
			}
		})
	}
}

func (s *CreateCmdSuite) TestTimeVersion() {
	ts := time.Date(2000, 12, 25, 00, 01, 02, 3456789, time.UTC)
	tsUnixStr := strconv.FormatInt(ts.Unix(), 10)
	tsUnixNanoStr := strconv.FormatInt(ts.UnixNano(), 10)

	cases := []struct {
		tid         string
		time        time.Time
		format      string
		expected    string
		expectedErr error
	}{
		{"Bad format", ts, "", "", errInvalidTimeFormat},
		{"unix", ts, "unix", tsUnixStr, nil},
		{"unixNano", ts, "unixNano", tsUnixNanoStr, nil},
		{"custom ymthms", ts, "20060102150405", "20001225000102", nil},
	}

	for _, c := range cases {
		s.Run(c.tid, func() {
			v, err := timeVersion(c.time, c.format)

			if c.expectedErr != nil {
				s.EqualError(err, c.expectedErr.Error())
			} else {
				s.NoError(err)
				s.Equal(c.expected, v)
			}
		})
	}
}

// TestCreateCmd tests function createCmd.
//
// For each test case, it creates a temp dir as "sandbox" (called `baseDir`) and
// all path manipulations are relative to `baseDir`.
func (s *CreateCmdSuite) TestCreateCmd() {
	ts := time.Date(2000, 12, 25, 00, 01, 02, 3456789, time.UTC)
	tsUnixStr := strconv.FormatInt(ts.Unix(), 10)
	tsUnixNanoStr := strconv.FormatInt(ts.UnixNano(), 10)
	testCwd := s.mustGetwd()

	cases := []struct {
		tid           string
		existingDirs  []string // directory paths to create before test. relative to baseDir.
		cwd           string   // path to chdir to before test. relative to baseDir.
		existingFiles []string // file paths created before test. relative to baseDir.
		expectedFiles []string // file paths expected to exist after test. paths relative to baseDir.
		expectedErr   error
		dir           string // `dir` parameter. if absolute path, will be converted to baseDir/dir.
		startTime     time.Time
		format        string
		seq           bool
		seqDigits     int
		ext           string
		name          string
	}{
		{"seq and format", nil, "", nil, nil, errIncompatibleSeqAndFormat, ".", ts, "unix", true, 4, "sql", "name"},
		{"seq init dir dot", nil, "", nil, []string{"0001_name.up.sql", "0001_name.down.sql"}, nil, ".", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir dot trailing slash", nil, "", nil, []string{"0001_name.up.sql", "0001_name.down.sql"}, nil, "./", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir double dot", []string{"subdir"}, "subdir", nil, []string{"0001_name.up.sql", "0001_name.down.sql"}, nil, "..", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir double dot trailing slash", []string{"subdir"}, "subdir", nil, []string{"0001_name.up.sql", "0001_name.down.sql"}, nil, "../", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir absolute", []string{"subdir"}, "", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "/subdir", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir absolute trailing slash", []string{"subdir"}, "", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "/subdir/", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir relative", []string{"subdir"}, "", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "subdir", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir relative trailing slash", []string{"subdir"}, "", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "subdir/", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir dot relative", []string{"subdir"}, "", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "./subdir", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir dot relative trailing slash", []string{"subdir"}, "", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "./subdir/", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir double dot relative", []string{"subdir"}, "subdir", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "../subdir", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir double dot relative trailing slash", []string{"subdir"}, "subdir", nil, []string{"subdir/0001_name.up.sql", "subdir/0001_name.down.sql"}, nil, "../subdir/", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq init dir maze", []string{"subdir"}, "subdir", nil, []string{"0001_name.up.sql", "0001_name.down.sql"}, nil, "..//subdir/./.././/subdir/..", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq width invalid", nil, "", nil, nil, errInvalidSequenceWidth, ".", ts, defaultTimeFormat, true, 0, "sql", "name"},
		{"seq malformed", nil, "", []string{"bad.sql"}, []string{"bad.sql"}, errors.New("Malformed migration filename: bad.sql"), ".", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq not int", nil, "", []string{"bad_bad.sql"}, []string{"bad_bad.sql"}, errors.New(`strconv.ParseUint: parsing "bad": invalid syntax`), ".", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq negative", nil, "", []string{"-5_negative.sql"}, []string{"-5_negative.sql"}, errors.New(`strconv.ParseUint: parsing "-5": invalid syntax`), ".", ts, defaultTimeFormat, true, 4, "sql", "name"},
		{"seq increment", nil, "", []string{"3_three.sql", "4_four.sql"}, []string{"3_three.sql", "4_four.sql", "0005_five.up.sql", "0005_five.down.sql"}, nil, ".", ts, defaultTimeFormat, true, 4, "sql", "five"},
		{"seq overflow", nil, "", []string{"9_nine.sql"}, []string{"9_nine.sql"}, errors.New(`Next sequence number 10 too large. At most 1 digits are allowed`), ".", ts, defaultTimeFormat, true, 1, "sql", "ten"},
		{"time empty format", nil, "", nil, nil, errInvalidTimeFormat, ".", ts, "", false, 0, "sql", "name"},
		{"time unix", nil, "", nil, []string{tsUnixStr + "_name.up.sql", tsUnixStr + "_name.down.sql"}, nil, ".", ts, "unix", false, 0, "sql", "name"},
		{"time unixNano", nil, "", nil, []string{tsUnixNanoStr + "_name.up.sql", tsUnixNanoStr + "_name.down.sql"}, nil, ".", ts, "unixNano", false, 0, "sql", "name"},
		{"time custom format", nil, "", nil, []string{"20001225000102_name.up.sql", "20001225000102_name.down.sql"}, nil, ".", ts, "20060102150405", false, 0, "sql", "name"},
		{"time version collision", nil, "", []string{"20001225_name.up.sql", "20001225_name.down.sql"}, []string{"20001225_name.up.sql", "20001225_name.down.sql"}, errors.New("duplicate migration version: 20001225"), ".", ts, "20060102", false, 0, "sql", "name"},
		{"dir invalid", nil, "", []string{"file"}, []string{"file"}, errors.New("mkdir 'test: this is invalid dir name'\x00: invalid argument"), "'test: this is invalid dir name'\000", ts, "unix", false, 0, "sql", "name"},
	}

	for _, c := range cases {
		s.Run(c.tid, func() {
			baseDir := s.mustCreateTempDir()

			for _, d := range c.existingDirs {
				s.mustCreateDir(filepath.Join(baseDir, d))
			}

			cwd := baseDir

			if c.cwd != "" {
				cwd = filepath.Join(baseDir, c.cwd)
			}

			s.mustChdir(cwd)

			for _, f := range c.existingFiles {
				s.mustWriteFile(baseDir, f, "")
			}

			dir := c.dir
			dir = filepath.ToSlash(dir)
			volName := filepath.VolumeName(baseDir)
			// Windows specific, can not recognize \subdir as abs path
			isWindowsAbsPathNoLetter := strings.HasPrefix(dir, "/") && volName != ""
			isRealAbsPath := filepath.IsAbs(dir)
			if isWindowsAbsPathNoLetter || isRealAbsPath {
				dir = filepath.Join(baseDir, dir)
			}

			err := createCmd(dir, c.startTime, c.format, c.name, c.ext, c.seq, c.seqDigits, false)

			if c.expectedErr != nil {
				s.EqualError(err, c.expectedErr.Error())
			} else {
				s.NoError(err)
			}

			if len(c.expectedFiles) == 0 {
				s.assertEmptyDir(baseDir)
			} else {
				for _, f := range c.expectedFiles {
					s.FileExists(filepath.Join(baseDir, f))
				}
			}

			s.mustChdir(testCwd)
			s.mustRemoveDir(baseDir)
		})
	}
}

func TestNumDownFromArgs(t *testing.T) {
	cases := []struct {
		name                string
		args                []string
		applyAll            bool
		expectedNeedConfirm bool
		expectedNum         int
		expectedErrStr      string
	}{
		{"no args", []string{}, false, true, -1, ""},
		{"down all", []string{}, true, false, -1, ""},
		{"down 5", []string{"5"}, false, false, 5, ""},
		{"down N", []string{"N"}, false, false, 0, "can't read limit argument N"},
		{"extra arg after -all", []string{"5"}, true, false, 0, "-all cannot be used with other arguments"},
		{"extra arg before -all", []string{"5", "-all"}, false, false, 0, "too many arguments"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			num, needsConfirm, err := numDownMigrationsFromArgs(c.applyAll, c.args)
			if needsConfirm != c.expectedNeedConfirm {
				t.Errorf("Incorrect needsConfirm was: %v wanted %v", needsConfirm, c.expectedNeedConfirm)
			}

			if num != c.expectedNum {
				t.Errorf("Incorrect num was: %v wanted %v", num, c.expectedNum)
			}

			if err != nil {
				if err.Error() != c.expectedErrStr {
					t.Error("Incorrect error: " + err.Error() + " != " + c.expectedErrStr)
				}
			} else if c.expectedErrStr != "" {
				t.Error("Expected error: " + c.expectedErrStr + " but got nil instead")
			}
		})
	}
}
