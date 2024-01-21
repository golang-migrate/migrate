package ydb

import "testing"

func TestSkipComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty input", "", ""},
		{"no comments", "SELECT * FROM table;", "SELECT * FROM table;"},
		{"single line comments", "-- This is a comment\nSELECT * FROM table;", "SELECT * FROM table;"},
		{"multi-line comments", "/* This is a comment */SELECT * FROM table;", "SELECT * FROM table;"},
		{"mixed comments", "-- Single line comment\n/* Multi\nLine\nComment */SELECT * FROM table;\nDROP TABLE table;", "SELECT * FROM table;\nDROP TABLE table;"},
		{"comments at the start", "-- Comment\nSELECT * FROM table;", "SELECT * FROM table;"},
		{"comments at the end", "SELECT * FROM table;-- Comment", "SELECT * FROM table;"},
		{"comments with special characters", "/* Com!ment */SELECT * FROM table;", "SELECT * FROM table;"},
		{"single line comment at end of line", "SELECT * FROM table; -- Comment", "SELECT * FROM table; "},
		{"multi-line comment in middle of line", "SELECT /* Comment */ * FROM table;", "SELECT  * FROM table;"},
		{"multiple single line comments consecutively", "-- Comment 1\n-- Comment 2\nSELECT * FROM table;", "SELECT * FROM table;"},
		{"multiple multi-line comments consecutively", "/* Comment 1 *//* Comment 2 */SELECT * FROM table;", "SELECT * FROM table;"},
		{"mixed comments in single line", "SELECT * -- Comment\nFROM /* Comment */table;", "SELECT * FROM table;"},
		{"comments with sql keywords", "-- SELECT * FROM table\nSELECT name FROM users;", "SELECT name FROM users;"},
		{"sql commands with comment-like syntax", "SELECT * FROM `--table--` WHERE name = '/* John */';", "SELECT * FROM `--table--` WHERE name = '/* John */';"},
		{"whitespace handling", "   -- Comment\nSELECT * FROM table;   ", "   SELECT * FROM table;   "},
		{"comments with escape characters", "-- Comment \n\t SELECT * FROM table;", "\t SELECT * FROM table;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := skipComments(tt.input)
			if err != nil {
				t.Errorf("skipComments() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("skipComments() = %v, want %v", result, tt.expected)
			}
		})
	}
}
