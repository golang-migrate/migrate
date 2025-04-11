//go:build !(aws_s3 || bitbucket || cassandra || clickhouse || cockroachdb || firebird || github || gitlab || go_bindata || godoc_vfs || google_cloud_storage || mongodb || mysql || neo4j || pgx || pgx5 || postgres || ql || redshift || rqlite || snowflake || spanner || sqlcipher || sqlite || sqlite3 || sqlserver || yugabytedb)
// +build !aws_s3,!bitbucket,!cassandra,!clickhouse,!cockroachdb,!firebird,!github,!gitlab,!go_bindata,!godoc_vfs,!google_cloud_storage,!mongodb,!mysql,!neo4j,!pgx,!pgx5,!postgres,!ql,!redshift,!rqlite,!snowflake,!spanner,!sqlcipher,!sqlite,!sqlite3,!sqlserver,!yugabytedb

package cli

import (
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/golang-migrate/migrate/v4/database/cassandra"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/database/cockroachdb"
	_ "github.com/golang-migrate/migrate/v4/database/firebird"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/database/neo4j"
	_ "github.com/golang-migrate/migrate/v4/database/pgx"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/database/ql"
	_ "github.com/golang-migrate/migrate/v4/database/redshift"
	_ "github.com/golang-migrate/migrate/v4/database/rqlite"
	_ "github.com/golang-migrate/migrate/v4/database/snowflake"
	_ "github.com/golang-migrate/migrate/v4/database/spanner"
	_ "github.com/golang-migrate/migrate/v4/database/sqlcipher"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/database/sqlserver"
	_ "github.com/golang-migrate/migrate/v4/database/yugabytedb"
	_ "github.com/golang-migrate/migrate/v4/source/aws_s3"
	_ "github.com/golang-migrate/migrate/v4/source/bitbucket"
	_ "github.com/golang-migrate/migrate/v4/source/github_ee"
	_ "github.com/golang-migrate/migrate/v4/source/gitlab"
	_ "github.com/golang-migrate/migrate/v4/source/go_bindata"
	_ "github.com/golang-migrate/migrate/v4/source/godoc_vfs"
	_ "github.com/golang-migrate/migrate/v4/source/google_cloud_storage"
)
