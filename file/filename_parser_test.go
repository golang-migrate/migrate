package file

import (
	"github.com/dimag-jfrog/migrate/migrate/direction"
	"testing"
)


type ParsingTest struct {
	filename          string
	filenameExtension string
	expectVersion     uint64
	expectName        string
	expectDirection   direction.Direction
	expectErr         bool
}

func testParser(t *testing.T, parser FilenameParser, test *ParsingTest) {
	version, name, migrate, err := parser.Parse(test.filename)
	if test.expectErr && err == nil {
		t.Fatal("Expected error, but got none.", test)
	}
	if !test.expectErr && err != nil {
		t.Fatal("Did not expect error, but got one:", err, test)
	}
	if err == nil {
		if version != test.expectVersion {
			t.Error("Wrong version number", test)
		}
		if name != test.expectName {
			t.Error("wrong name", test)
		}
		if migrate != test.expectDirection {
			t.Error("wrong migrate", test)
		}
	}
}

func TestParseDefaultFilenameSchema(t *testing.T) {
	var tests = []ParsingTest {
		{"001_test_file.up.sql", "sql", 1, "test_file", direction.Up, false},
		{"001_test_file.down.sql", "sql", 1, "test_file", direction.Down, false},
		{"10034_test_file.down.sql", "sql", 10034, "test_file", direction.Down, false},
		{"-1_test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file.down", "sql", 0, "", direction.Up, true},
		{"100_test_file.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file", "sql", 0, "", direction.Up, true},
		{"test_file", "sql", 0, "", direction.Up, true},
		{"100", "sql", 0, "", direction.Up, true},
		{".sql", "sql", 0, "", direction.Up, true},
		{"up.sql", "sql", 0, "", direction.Up, true},
		{"down.sql", "sql", 0, "", direction.Up, true},
	}

	for _, test := range tests {
		parser := DefaultFilenameParser{FilenameExtension:test.filenameExtension}
		testParser(t, &parser, &test)
	}
}

func TestParseUpDownAndBothFilenameSchema(t *testing.T) {
	var tests = []ParsingTest {
		{"001_test_file.up.sql", "sql", 1, "test_file", direction.Up, false},
		{"001_test_file.down.sql", "sql", 1, "test_file", direction.Down, false},
		{"10034_test_file.down.sql", "sql", 10034, "test_file", direction.Down, false},
		{"-1_test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file.down", "sql", 0, "", direction.Up, true},
		{"100_test_file.sql", "sql", 100, "test_file", direction.Both, false},
		{"001_test_file.mgo", "mgo", 1, "test_file", direction.Both, false},
		{"-1_test_file.mgo", "sql", 0, "", direction.Up, true},
		{"100_test_file", "sql", 0, "", direction.Up, true},
		{"test_file", "sql", 0, "", direction.Up, true},
		{"100", "sql", 0, "", direction.Up, true},
		{".sql", "sql", 0, "", direction.Up, true},
		{"up.sql", "sql", 0, "", direction.Up, true},
		{"down.sql", "sql", 0, "", direction.Up, true},
	}

	for _, test := range tests {
		parser := UpDownAndBothFilenameParser{FilenameExtension:test.filenameExtension}
		testParser(t, &parser, &test)
	}
}
