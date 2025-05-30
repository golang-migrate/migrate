package ddl

import (
	"fmt"
	"strings"
)

func ToMigrationStatements(path, ddl string) ([]string, error) {
	_, err := parser.ParseString(path, ddl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDL: %w", err)
	}

	return nil, nil
}

func (d *SpannerDDL) String() string {
	out := ""

	for _, s := range d.Statements {
		out += s.String() + "\n"
	}

	return out
}

func (s *Statement) String() string {
	return s.Expression.String() + s.End
}

func (e *Expression) String() string {
	list := []string{}

	for _, v := range e.Values {
		list = append(list, v.String())
	}

	return strings.Join(list, " ")
}

func (v *Value) String() string {
	out := ""

	if v.Braced != nil {
		out += "(" + v.Braced.String() + ")"
	} else if v.Bracketed != nil {
		out += "<" + v.Bracketed.String() + ">"
	} else {
		out += strings.Join(v.Component, " ")
	}

	return out
}
