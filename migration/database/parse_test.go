package database_test

import (
	"encoding/hex"
	"net/url"
	"strings"
	"testing"
)

const reservedChars = "!#$%&'()*+,/:;=?@[]"

const baseUsername = "username"

// TestUserUnencodedReservedURLChars documents the behavior of using unencoded reserved characters in usernames with
// net/url Parse()
func TestUserUnencodedReservedURLChars(t *testing.T) {
	scheme := "database://"
	urlSuffix := "password@localhost:12345/myDB?someParam=true"
	urlSuffixAndSep := ":" + urlSuffix

	testcases := []struct {
		char             string
		parses           bool
		expectedUsername string // empty string means that the username failed to parse
		encodedURL       string
	}{
		{char: "!", parses: true, expectedUsername: baseUsername + "!",
			encodedURL: scheme + baseUsername + "%21" + urlSuffixAndSep},
		{char: "#", parses: true, expectedUsername: "",
			encodedURL: scheme + baseUsername + "#" + urlSuffixAndSep},
		{char: "$", parses: true, expectedUsername: baseUsername + "$",
			encodedURL: scheme + baseUsername + "$" + urlSuffixAndSep},
		{char: "%", parses: false},
		{char: "&", parses: true, expectedUsername: baseUsername + "&",
			encodedURL: scheme + baseUsername + "&" + urlSuffixAndSep},
		{char: "'", parses: true, expectedUsername: "username'",
			encodedURL: scheme + baseUsername + "%27" + urlSuffixAndSep},
		{char: "(", parses: true, expectedUsername: "username(",
			encodedURL: scheme + baseUsername + "%28" + urlSuffixAndSep},
		{char: ")", parses: true, expectedUsername: "username)",
			encodedURL: scheme + baseUsername + "%29" + urlSuffixAndSep},
		{char: "*", parses: true, expectedUsername: "username*",
			encodedURL: scheme + baseUsername + "%2A" + urlSuffixAndSep},
		{char: "+", parses: true, expectedUsername: "username+",
			encodedURL: scheme + baseUsername + "+" + urlSuffixAndSep},
		{char: ",", parses: true, expectedUsername: "username,",
			encodedURL: scheme + baseUsername + "," + urlSuffixAndSep},
		{char: "/", parses: true, expectedUsername: "",
			encodedURL: scheme + baseUsername + "/" + urlSuffixAndSep},
		{char: ":", parses: true, expectedUsername: baseUsername,
			encodedURL: scheme + baseUsername + ":%3A" + urlSuffix},
		{char: ";", parses: true, expectedUsername: "username;",
			encodedURL: scheme + baseUsername + ";" + urlSuffixAndSep},
		{char: "=", parses: true, expectedUsername: "username=",
			encodedURL: scheme + baseUsername + "=" + urlSuffixAndSep},
		{char: "?", parses: true, expectedUsername: "",
			encodedURL: scheme + baseUsername + "?" + urlSuffixAndSep},
		{char: "@", parses: true, expectedUsername: "username@",
			encodedURL: scheme + baseUsername + "%40" + urlSuffixAndSep},
		{char: "[", parses: false},
		{char: "]", parses: false},
	}

	testedChars := make([]string, 0, len(reservedChars))
	for _, tc := range testcases {
		testedChars = append(testedChars, tc.char)
		t.Run("reserved char "+tc.char, func(t *testing.T) {
			s := scheme + baseUsername + tc.char + urlSuffixAndSep
			u, err := url.Parse(s)
			if err == nil {
				if !tc.parses {
					t.Error("Unexpectedly parsed reserved character. url:", s)
					return
				}
				var username string
				if u.User != nil {
					username = u.User.Username()
				}
				if username != tc.expectedUsername {
					t.Error("Got unexpected username:", username, "!=", tc.expectedUsername)
				}
				if s := u.String(); s != tc.encodedURL {
					t.Error("Got unexpected encoded URL:", s, "!=", tc.encodedURL)
				}
			} else {
				if tc.parses {
					t.Error("Failed to parse reserved character. url:", s)
				}
			}
		})
	}

	t.Run("All reserved chars tested", func(t *testing.T) {
		if s := strings.Join(testedChars, ""); s != reservedChars {
			t.Error("Not all reserved URL characters were tested:", s, "!=", reservedChars)
		}
	})
}

func TestUserEncodedReservedURLChars(t *testing.T) {
	scheme := "database://"
	urlSuffix := "password@localhost:12345/myDB?someParam=true"
	urlSuffixAndSep := ":" + urlSuffix

	for _, c := range reservedChars {
		c := string(c)
		t.Run("reserved char "+c, func(t *testing.T) {
			encodedChar := "%" + hex.EncodeToString([]byte(c))
			s := scheme + baseUsername + encodedChar + urlSuffixAndSep
			expectedUsername := baseUsername + c
			u, err := url.Parse(s)
			if err != nil {
				t.Fatal("Failed to parse url with encoded reserved character. url:", s)
			}
			if u.User == nil {
				t.Fatal("Failed to parse userinfo with encoded reserve character. url:", s)
			}
			if username := u.User.Username(); username != expectedUsername {
				t.Fatal("Got unexpected username:", username, "!=", expectedUsername)
			}
		})
	}
}

// TestPasswordUnencodedReservedURLChars documents the behavior of using unencoded reserved characters in passwords
// with net/url Parse()
func TestPasswordUnencodedReservedURLChars(t *testing.T) {
	username := baseUsername
	schemeAndUsernameAndSep := "database://" + username + ":"
	basePassword := "password"
	urlSuffixAndSep := "@localhost:12345/myDB?someParam=true"

	testcases := []struct {
		char             string
		parses           bool
		expectedUsername string // empty string means that the username failed to parse
		expectedPassword string // empty string means that the password failed to parse
		encodedURL       string
	}{
		{char: "!", parses: true, expectedUsername: username, expectedPassword: basePassword + "!",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%21" + urlSuffixAndSep},
		{char: "#", parses: false},
		{char: "$", parses: true, expectedUsername: username, expectedPassword: basePassword + "$",
			encodedURL: schemeAndUsernameAndSep + basePassword + "$" + urlSuffixAndSep},
		{char: "%", parses: false},
		{char: "&", parses: true, expectedUsername: username, expectedPassword: basePassword + "&",
			encodedURL: schemeAndUsernameAndSep + basePassword + "&" + urlSuffixAndSep},
		{char: "'", parses: true, expectedUsername: username, expectedPassword: "password'",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%27" + urlSuffixAndSep},
		{char: "(", parses: true, expectedUsername: username, expectedPassword: "password(",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%28" + urlSuffixAndSep},
		{char: ")", parses: true, expectedUsername: username, expectedPassword: "password)",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%29" + urlSuffixAndSep},
		{char: "*", parses: true, expectedUsername: username, expectedPassword: "password*",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%2A" + urlSuffixAndSep},
		{char: "+", parses: true, expectedUsername: username, expectedPassword: "password+",
			encodedURL: schemeAndUsernameAndSep + basePassword + "+" + urlSuffixAndSep},
		{char: ",", parses: true, expectedUsername: username, expectedPassword: "password,",
			encodedURL: schemeAndUsernameAndSep + basePassword + "," + urlSuffixAndSep},
		{char: "/", parses: false},
		{char: ":", parses: true, expectedUsername: username, expectedPassword: "password:",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%3A" + urlSuffixAndSep},
		{char: ";", parses: true, expectedUsername: username, expectedPassword: "password;",
			encodedURL: schemeAndUsernameAndSep + basePassword + ";" + urlSuffixAndSep},
		{char: "=", parses: true, expectedUsername: username, expectedPassword: "password=",
			encodedURL: schemeAndUsernameAndSep + basePassword + "=" + urlSuffixAndSep},
		{char: "?", parses: false},
		{char: "@", parses: true, expectedUsername: username, expectedPassword: "password@",
			encodedURL: schemeAndUsernameAndSep + basePassword + "%40" + urlSuffixAndSep},
		{char: "[", parses: false},
		{char: "]", parses: false},
	}

	testedChars := make([]string, 0, len(reservedChars))
	for _, tc := range testcases {
		testedChars = append(testedChars, tc.char)
		t.Run("reserved char "+tc.char, func(t *testing.T) {
			s := schemeAndUsernameAndSep + basePassword + tc.char + urlSuffixAndSep
			u, err := url.Parse(s)
			if err == nil {
				if !tc.parses {
					t.Error("Unexpectedly parsed reserved character. url:", s)
					return
				}
				var username, password string
				if u.User != nil {
					username = u.User.Username()
					password, _ = u.User.Password()
				}
				if username != tc.expectedUsername {
					t.Error("Got unexpected username:", username, "!=", tc.expectedUsername)
				}
				if password != tc.expectedPassword {
					t.Error("Got unexpected password:", password, "!=", tc.expectedPassword)
				}
				if s := u.String(); s != tc.encodedURL {
					t.Error("Got unexpected encoded URL:", s, "!=", tc.encodedURL)
				}
			} else {
				if tc.parses {
					t.Error("Failed to parse reserved character. url:", s)
				}
			}
		})
	}

	t.Run("All reserved chars tested", func(t *testing.T) {
		if s := strings.Join(testedChars, ""); s != reservedChars {
			t.Error("Not all reserved URL characters were tested:", s, "!=", reservedChars)
		}
	})
}

func TestPasswordEncodedReservedURLChars(t *testing.T) {
	username := baseUsername
	schemeAndUsernameAndSep := "database://" + username + ":"
	basePassword := "password"
	urlSuffixAndSep := "@localhost:12345/myDB?someParam=true"

	for _, c := range reservedChars {
		c := string(c)
		t.Run("reserved char "+c, func(t *testing.T) {
			encodedChar := "%" + hex.EncodeToString([]byte(c))
			s := schemeAndUsernameAndSep + basePassword + encodedChar + urlSuffixAndSep
			expectedPassword := basePassword + c
			u, err := url.Parse(s)
			if err != nil {
				t.Fatal("Failed to parse url with encoded reserved character. url:", s)
			}
			if u.User == nil {
				t.Fatal("Failed to parse userinfo with encoded reserve character. url:", s)
			}
			if n := u.User.Username(); n != username {
				t.Fatal("Got unexpected username:", n, "!=", username)
			}
			if p, _ := u.User.Password(); p != expectedPassword {
				t.Fatal("Got unexpected password:", p, "!=", expectedPassword)
			}
		})
	}
}
