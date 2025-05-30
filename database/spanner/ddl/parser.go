package ddl

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// SpannerDDL only parses the components that are relevant for the migration tool.
// Main objectives:
//   - Remove comments: spanner does allow comments in DDL.
//   - Split DDL into statements: spanner requires the migration request to be a list of statements, not a single DDL file.
//     Also each request should not exceed 10 statements.
type SpannerDDL struct {
	Statements []*Statement `@@*`
}

type Statement struct {
	Expression *Expression `@@`
	End        string      `@";"?`
}

type Expression struct {
	Values []*Value `@@+`
}

type Value struct {
	Component []string    `  @(Ident | String | Comma)+`
	Braced    *Expression `| "(" @@ ")"`
	Bracketed *Expression `| "<" @@ ">"`
}

var parser = participle.MustBuild[SpannerDDL](
	participle.Lexer(lexer.MustSimple([]lexer.SimpleRule{
		{"SingleLineComment", `--[^\n]*`},
		{"MultiLineComment", `(?s)/\*.*?\*/`},
		{"Whitespace", `\s+`},

		{"StatementEnd", `;`},
		{"Group", `[()<>]`},

		{"Ident", `[a-zA-Z\d_]+`},
		{"String", `"(?:\\.|[^"])*"`},
		{"Comma", `,`},
	})),
	participle.Elide("SingleLineComment", "MultiLineComment", "Whitespace"),
)
