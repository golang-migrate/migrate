package cli

import (
	"testing"
)

func TestCleanDir(t *testing.T) {
	cases := []struct {
		dir              string
		expectedCleanDir string
	}{
		{dir: "", expectedCleanDir: ""},
		{dir: ".", expectedCleanDir: ""},
		{dir: "/", expectedCleanDir: "/"},
		{dir: "./", expectedCleanDir: ""},
		{dir: ".test", expectedCleanDir: ".test/"},
		{dir: ".test/", expectedCleanDir: ".test/"},
		{dir: "test", expectedCleanDir: "test/"},
		{dir: "test/", expectedCleanDir: "test/"},
		{dir: "./test", expectedCleanDir: "test/"},
		{dir: "./test/", expectedCleanDir: "test/"},
		{dir: "test/test", expectedCleanDir: "test/test/"},
		{dir: "test/test/", expectedCleanDir: "test/test/"},
		{dir: "./test/test", expectedCleanDir: "test/test/"},
		{dir: "./test/test/", expectedCleanDir: "test/test/"},
	}

	for _, c := range cases {
		t.Run(c.dir, func(t *testing.T) {
			cleanedDir := cleanDir(c.dir)
			if cleanedDir != c.expectedCleanDir {
				t.Error("Incorrectly cleaned dir: " + cleanedDir + " != " + c.expectedCleanDir)
			}
		})
	}
}

func TestNextSeq(t *testing.T) {
	cases := []struct {
		name           string
		matches        []string
		dir            string
		seqDigits      int
		expected       string
		expectedErrStr string
	}{
		{"Bad digits", []string{}, "migrationDir/", 0, "", "Digits must be positive"},
		{"Single digit initialize", []string{}, "migrationDir/", 1, "1", ""},
		{"Single digit malformed", []string{"bad"}, "migrationDir/", 1, "", "Malformed migration filename: bad"},
		{"Single digit no int", []string{"bad_bad"}, "migrationDir/", 1, "", "strconv.Atoi: parsing \"bad\": invalid syntax"},
		{"Single digit negative seq", []string{"-5_test"}, "migrationDir/", 1, "", "Next sequence number must be positive"},
		{"Single digit increment", []string{"3_test", "4_test"}, "migrationDir/", 1, "5", ""},
		{"Single digit overflow", []string{"9_test"}, "migrationDir/", 1, "", "Next sequence number 10 too large. At most 1 digits are allowed"},
		{"Zero-pad initialize", []string{}, "migrationDir/", 6, "000001", ""},
		{"Zero-pad malformed", []string{"bad"}, "migrationDir/", 6, "", "Malformed migration filename: bad"},
		{"Zero-pad no int", []string{"bad_bad"}, "migrationDir/", 6, "", "strconv.Atoi: parsing \"bad\": invalid syntax"},
		{"Zero-pad negative seq", []string{"-000005_test"}, "migrationDir/", 6, "", "Next sequence number must be positive"},
		{"Zero-pad increment", []string{"000003_test", "000004_test"}, "migrationDir/", 6, "000005", ""},
		{"Zero-pad overflow", []string{"999999_test"}, "migrationDir/", 6, "", "Next sequence number 1000000 too large. At most 6 digits are allowed"},
		{"dir - no trailing slash", []string{"migrationDir/000001_test"}, "migrationDir", 6, "", `strconv.Atoi: parsing "/000001": invalid syntax`},
		{"dir - with dot prefix success", []string{"migrationDir/000001_test"}, "./migrationDir/", 6, "", `strconv.Atoi: parsing "migrationDir/000001": invalid syntax`},
		{"dir - no dir prefix", []string{"000001_test"}, "migrationDir/", 6, "000002", ""},
		{"dir - strip success", []string{"migrationDir/000001_test"}, "migrationDir/", 6, "000002", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			nextSeq, err := nextSeq(c.matches, c.dir, c.seqDigits)
			if nextSeq != c.expected {
				t.Error("Incorrect nextSeq: " + nextSeq + " != " + c.expected)
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
