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

func TestToMigrationStatements(t *testing.T) {
	g := got.T(t)

	list, err := ToMigrationStatements("", `
		-- Comment
		CREATE TABLE test(id INT64);

		CREATE PROPERTY GRAPH SocialGraph
		NODE TABLES (Person)
		EDGE TABLES (Knows);
	`)
	g.E(err)

	g.Snapshot("list", list)
}
