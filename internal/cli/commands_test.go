package cli

import (
	"testing"
	"time"
)

func TestNextSeqVersion(t *testing.T) {
	cases := []struct {
		name        string
		matches     []string
		seqDigits   int
		expected    string
		expectedErr string
	}{
		{"Bad digits", []string{}, 0, "", errInvalidSequenceWidth.Error()},
		{"Single digit initialize", []string{}, 1, "1", ""},
		{"Single digit malformed", []string{"bad"}, 1, "", "Malformed migration filename: bad"},
		{"Single digit no int", []string{"bad_bad"}, 1, "", `strconv.ParseUint: parsing "bad": invalid syntax`},
		{"Single digit negative seq", []string{"-5_test"}, 1, "", `strconv.ParseUint: parsing "-5": invalid syntax`},
		{"Single digit increment", []string{"3_test", "4_test"}, 1, "5", ""},
		{"Single digit overflow", []string{"9_test"}, 1, "", "Next sequence number 10 too large. At most 1 digits are allowed"},
		{"Zero-pad initialize", []string{}, 6, "000001", ""},
		{"Zero-pad malformed", []string{"bad"}, 6, "", "Malformed migration filename: bad"},
		{"Zero-pad no int", []string{"bad_bad"}, 6, "", `strconv.ParseUint: parsing "bad": invalid syntax`},
		{"Zero-pad negative seq", []string{"-000005_test"}, 6, "", `strconv.ParseUint: parsing "-000005": invalid syntax`},
		{"Zero-pad increment", []string{"000003_test", "000004_test"}, 6, "000005", ""},
		{"Zero-pad overflow", []string{"999999_test"}, 6, "", "Next sequence number 1000000 too large. At most 6 digits are allowed"},
		{"dir absolute path", []string{"/migrationDir/000001_test"}, 6, "000002", ""},
		{"dir relative path", []string{"migrationDir/000001_test"}, 6, "000002", ""},
		{"dir dot prefix", []string{"./migrationDir/000001_test"}, 6, "000002", ""},
		{"dir parent prefix", []string{"../migrationDir/000001_test"}, 6, "000002", ""},
		{"dir no prefix", []string{"000001_test"}, 6, "000002", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, err := nextSeqVersion(c.matches, c.seqDigits)

			if err == nil {
				if c.expectedErr != "" {
					t.Errorf("Expected error: %q, but got nil instead.", c.expectedErr)
				} else {
					if v != c.expected {
						t.Errorf("Incorrect version %q. Expected %q.", v, c.expected)
					}
				}
			} else {
				if err.Error() != c.expectedErr {
					t.Errorf("Incorrect error %q. Expected: %q.", err.Error(), c.expectedErr)
				}
			}
		})
	}
}

func TestTimeVersion(t *testing.T) {
	ts := time.Date(2000, 12, 25, 00, 01, 02, 3456789, time.UTC)

	cases := []struct {
		name        string
		time        time.Time
		format      string
		expected    string
		expectedErr string
	}{
		{"Bad format", ts, "", "", errInvalidTimeFormat.Error()},
		{"unix", ts, "unix", "977702462", ""},
		{"unixNano", ts, "unixNano", "977702462003456789", ""},
		{"custom ymthms", ts, "20060102150405", "20001225000102", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, err := timeVersion(c.time, c.format)

			if err == nil {
				if c.expectedErr != "" {
					t.Errorf("Expected error: %q, but got nil instead.", c.expectedErr)
				} else {
					if v != c.expected {
						t.Errorf("Incorrect version %q. Expected %q.", v, c.expected)
					}
				}
			} else {
				if err.Error() != c.expectedErr {
					t.Errorf("Incorrect error %q. Expected: %q.", err.Error(), c.expectedErr)
				}
			}
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
