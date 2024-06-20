package spanner

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	projectId    = "abc"
	instanceId   = "edf"
	databaseName = "testdb"
)

var (
	opts  = dktest.Options{PortRequired: true, ReadyFunc: isReady}
	specs = []dktesting.ContainerSpec{
		{ImageName: "gcr.io/cloud-spanner-emulator/emulator", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	// Spanner exposes 2 ports (9010 for gRPC, 9020 for REST)
	// We only need the port bound to 9010
	ip, port, err := c.Port(9010)
	if err != nil {
		return false
	}
	ipAndPort := fmt.Sprintf("%s:%s", ip, port)
	_ = os.Setenv("SPANNER_EMULATOR_HOST", ipAndPort)

	if err := createInstance(ctx); err != nil {
		return false
	}

	if err := createDatabase(ctx); err != nil {
		return false
	}

	return true
}

func createInstance(ctx context.Context) error {
	ic, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = ic.Close()
	}()

	giReq := &instancepb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
	}
	if _, err := ic.GetInstance(ctx, giReq); err == nil {
		return nil // skip if instance already created
	}

	ciReq := &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectId),
		InstanceId: instanceId,
		Instance: &instancepb.Instance{
			Config:      "emu",
			DisplayName: instanceId,
			NodeCount:   1,
		},
	}
	op, err := ic.CreateInstance(ctx, ciReq)
	if err != nil {
		return err
	}

	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func createDatabase(ctx context.Context) error {
	ac, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = ac.Close()
	}()

	req := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseName),
	}
	op, err := ac.CreateDatabase(ctx, req)
	if err != nil {
		return err
	}

	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		uri := fmt.Sprintf("spanner://projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseName)
		s := &Spanner{}
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("CREATE TABLE test (id BOOL) PRIMARY KEY (id)"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		s := &Spanner{}
		uri := fmt.Sprintf("spanner://projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseName)
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", uri, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestCleanStatements(t *testing.T) {
	testCases := []struct {
		name           string
		multiStatement string
		expected       []string
	}{
		{
			name:           "no statement",
			multiStatement: "",
			expected:       []string{},
		},
		{
			name:           "single statement, single line, no semicolon, no comment",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)",
			expected:       []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name: "single statement, multi line, no semicolon, no comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY (id)`,
			expected: []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name:           "single statement, single line, with semicolon, no comment",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id);",
			expected:       []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name: "single statement, multi line, with semicolon, no comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY (id);`,
			expected: []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name: "multi statement, with trailing semicolon. no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id);`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "multi statement, no trailing semicolon, no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "multi statement, no trailing semicolon, standalone comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			-- standalone comment
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "multi statement, no trailing semicolon, inline comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL, -- inline comment
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "alter table with SET OPTIONS",
			multiStatement: `ALTER TABLE users ALTER COLUMN created
			SET OPTIONS (allow_commit_timestamp=true);`,
			expected: []string{"ALTER TABLE users ALTER COLUMN created SET OPTIONS (allow_commit_timestamp = true)"},
		},
		{
			name: "column with NUMERIC type",
			multiStatement: `CREATE TABLE table_name (
				id STRING(255) NOT NULL,
				sum NUMERIC,
			) PRIMARY KEY (id)`,
			expected: []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n  sum NUMERIC,\n) PRIMARY KEY(id)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := cleanStatements([]byte(tc.multiStatement))
			require.NoError(t, err, "Error cleaning statements")
			assert.Equal(t, tc.expected, stmts)
		})
	}
}
