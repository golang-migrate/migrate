package ddl

import (
	"testing"

	"github.com/ysmood/got"
)

func TestBasic(t *testing.T) {
	g := got.T(t)

	ddl, err := parser.ParseString("", `
		-- Comment
		A B;
		A;
		A (B C); /* 
			Multi-line
			comment
		*/
		A "B";
		A <B, C>;
		A -- Comment
		B,
		-- Comment
		A (B (C D)) E
	`)
	g.E(err)

	g.Snapshot("ddl", ddl)

	g.Snapshot("ddl-string", ddl.String())
}
