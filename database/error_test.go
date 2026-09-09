package database

import (
	"errors"
	"testing"
)

func TestRedactPassword(t *testing.T) {
	testcases := []struct {
		name     string
		input    error
		expected string
	}{
		{
			name:     "quoted password in key-value format",
			input:    errors.New("connection failed: password='secret123' invalid"),
			expected: "connection failed: password=xxxxx invalid",
		},
		{
			name:     "plain password in key-value format",
			input:    errors.New("connection failed: password=secret123 invalid"),
			expected: "connection failed: password=xxxxx invalid",
		},
		{
			name:     "password in URL format",
			input:    errors.New("connection failed: postgres://user:secret123@localhost/db"),
			expected: "connection failed: postgres://user:xxxxxx@localhost/db",
		},
		{
			name:     "multiple password formats",
			input:    errors.New("connection failed: password='secret' and url postgres://user:pass@host"),
			expected: "connection failed: password=xxxxx and url postgres://user:xxxxxx@host",
		},
		{
			name:     "no password in error",
			input:    errors.New("connection failed: invalid host"),
			expected: "connection failed: invalid host",
		},
		{
			name:     "empty error",
			input:    errors.New(""),
			expected: "",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result := RedactPassword(tc.input)
			if result.Error() != tc.expected {
				t.Errorf("Expected %q, got %q", tc.expected, result.Error())
			}
		})
	}
}

type SpecialError struct {
	msg string
}

func (e SpecialError) Error() string {
	return e.msg
}

func TestRedactPasswordPreservesOriginalWhenNoPassword(t *testing.T) {
	originalErr := SpecialError{msg: "no password here"}
	result := RedactPassword(originalErr)

	if !errors.Is(result, originalErr) {
		t.Error("Expected original error to be returned when no password found")
	}
}
