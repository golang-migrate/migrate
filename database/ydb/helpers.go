package ydb

import (
	"regexp"
	"strings"
)

const (
	createVersionTableQueryTemplate = `
	CREATE TABLE %s (
		version Int32,
		dirty Bool,
		applied_at Timestamp,
		PRIMARY KEY (version)
	);
	`

	deleteVersionsQueryTemplate = `
	DELETE FROM %s;`

	setVersionQueryTemplate = `
	DECLARE $version AS Int32;
	DECLARE $dirty AS Bool;
	DECLARE $applied_at AS Timestamp;
	UPSERT INTO %s (version, dirty, applied_at) 
	VALUES ($version, $dirty, $applied_at);`

	getVersionQueryTemplate = `
	SELECT version, dirty FROM %s 
	ORDER BY version DESC LIMIT 1;`

	dropTablesQueryTemplate = "DROP TABLE `%s`;"
)

type queryMode int

const (
	notSetMode queryMode = iota
	ddlMode
	dmlMode
	unknownMode
)

func skipComments(statements string) (string, error) {
	type interval struct {
		start int
		end   int
	}
	comments := make([]interval, 0)
	inString := false
	stringDelimiter := byte(0)

	for i := 0; i < len(statements); i++ {
		if (statements[i] == '\'' || statements[i] == '`') && (i == 0 || statements[i-1] != '\\') {
			if inString {
				if statements[i] == stringDelimiter {
					inString = false
				}
			} else {
				inString = true
				stringDelimiter = statements[i]
			}
		}

		if !inString {
			if statements[i] == '-' && i+1 < len(statements) && statements[i+1] == '-' {
				start := i
				for ; i < len(statements) && statements[i] != '\n'; i++ {
				}
				comments = append(comments, interval{start: start, end: i})
			} else if statements[i] == '/' && i+1 < len(statements) && statements[i+1] == '*' {
				start := i
				for ; i < len(statements)-1 && !(statements[i] == '*' && statements[i+1] == '/'); i++ {
				}
				comments = append(comments, interval{start: start, end: i + 1})
			}
		}
	}

	res := strings.Builder{}
	curPos := 0

	for _, comment := range comments {
		_, err := res.WriteString(statements[curPos:comment.start])
		if err != nil {
			return "", err
		}
		curPos = comment.end + 1
	}

	if curPos < len(statements) {
		_, err := res.WriteString(statements[curPos:])
		if err != nil {
			return "", err
		}
	}

	return res.String(), nil
}

func detectQueryMode(statement string) queryMode {
	ddlReg := regexp.MustCompile(`^(?i)(CREATE|ALTER|DECLARE|GRANT|REVOKE|DROP).*`)
	if ddlReg.MatchString(statement) {
		return ddlMode
	}

	return dmlMode
}
