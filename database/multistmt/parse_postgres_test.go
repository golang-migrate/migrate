package multistmt_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4/database/multistmt"
)

func TestPGParse(t *testing.T) {
	createFunctionEmptyTagStmt := `CREATE FUNCTION set_new_id() RETURNS TRIGGER AS
$$
BEGIN
	NEW.new_id := NEW.id;
	RETURN NEW;
END
$$ LANGUAGE PLPGSQL;`

	createFunctionStmt := `CREATE FUNCTION set_new_id() RETURNS TRIGGER AS
$BODY$
BEGIN
	NEW.new_id := NEW.id;
	RETURN NEW;
END
$BODY$ LANGUAGE PLPGSQL;`

	createTriggerStmt := `CREATE TRIGGER set_new_id_trigger BEFORE INSERT OR UPDATE ON mytable
FOR EACH ROW EXECUTE PROCEDURE set_new_id();`

	nestedDollarQuotes := `$function$
BEGIN
    RETURN ($1 ~ $q$[\t\r\n\v\\]$q$);
END;
$function$;`

	advancedCreateFunction := `CREATE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$
DECLARE passed BOOLEAN;
BEGIN
        SELECT  (pwd = $2) INTO passed
        FROM    pwds
        WHERE   username = $1;

        RETURN passed;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'pg_temp'.
    SET search_path = admin, pg_temp;`

	testCases := []struct {
		name        string
		multiStmt   string
		delimiter   string
		expected    []string
		expectedErr error
	}{
		{name: "single statement, no delimiter", multiStmt: "single statement, no delimiter", delimiter: ";",
			expected: []string{"single statement, no delimiter"}, expectedErr: nil},
		{name: "single statement, one delimiter", multiStmt: "single statement, one delimiter;", delimiter: ";",
			expected: []string{"single statement, one delimiter;"}, expectedErr: nil},
		{name: "two statements, no trailing delimiter", multiStmt: "statement one; statement two", delimiter: ";",
			expected: []string{"statement one;", " statement two"}, expectedErr: nil},
		{name: "two statements, with trailing delimiter", multiStmt: "statement one; statement two;", delimiter: ";",
			expected: []string{"statement one;", " statement two;"}, expectedErr: nil},
		{name: "singe statement with nested dollar-quoted string", multiStmt: nestedDollarQuotes, delimiter: ";",
			expected: []string{nestedDollarQuotes}},
		{name: "three statements with dollar-quoted strings", multiStmt: strings.Join([]string{createFunctionStmt,
			createFunctionEmptyTagStmt, advancedCreateFunction, createTriggerStmt, nestedDollarQuotes}, ""),
			delimiter: ";", expected: []string{createFunctionStmt, createFunctionEmptyTagStmt, advancedCreateFunction,
			createTriggerStmt, nestedDollarQuotes}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts := make([]string, 0, len(tc.expected))
			err := multistmt.PGParse(strings.NewReader(tc.multiStmt), []byte(tc.delimiter), maxMigrationSize, func(b []byte) bool {
				stmts = append(stmts, string(b))
				return true
			})
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expected, stmts)
		})
	}
}

func TestPGParseDiscontinue(t *testing.T) {
	multiStmt := "statement one; statement two"
	delimiter := ";"
	expected := []string{"statement one;"}

	stmts := make([]string, 0, len(expected))
	err := multistmt.PGParse(strings.NewReader(multiStmt), []byte(delimiter), maxMigrationSize, func(b []byte) bool {
		stmts = append(stmts, string(b))
		return false
	})
	assert.Nil(t, err)
	assert.Equal(t, expected, stmts)
}
