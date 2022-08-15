package migrate

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func Test_applyEnvironmentTemplate(t *testing.T) {
	envMapCache = nil
	_ = os.Setenv("WAREHOUSE_DB", "WH_STAGING")

	migration := io.NopCloser(bytes.NewBuffer([]byte(`SELECT * FROM {{.WAREHOUSE_DB}}.STD.INVOICES`)))

	gotReader, err := applyEnvironmentTemplate(migration)
	if err != nil {
		t.Fatalf("expected no error applying template")
	}

	gotBytes, err := io.ReadAll(gotReader)
	if err != nil {
		t.Fatalf("expected no error reading")
	}
	got := string(gotBytes)
	want := `SELECT * FROM WH_STAGING.STD.INVOICES`
	if got != want {
		t.Fatalf("expected [%s] but got [%s]", want, got)
	}
}
