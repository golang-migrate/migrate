package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"os"
)

type App struct {
	Connection     *sql.Conn
	MigrationTable string
	HistoryID      *int64
}

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/db?multiStatements=true")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	defer db.Close()

	app := &App{
		Connection:     conn,
		MigrationTable: mysql.DefaultMigrationsTable,
	}

	databaseDrv, err := mysql.WithConnection(ctx, conn, &mysql.Config{
		DatabaseName: "db",
		Triggers: map[string]func(response interface{}) error{
			database.TrigVersionTableExists: app.MigrationHistoryTable,
			database.TrigVersionTablePost:   app.MigrationHistoryTable,
			database.TrigRunPost:            app.DatabaseRunPost,
		},
	})
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	m, err := migrate.NewFromOptions(migrate.Options{
		DatabaseInstance: databaseDrv,
		DatabaseName:     "db",
		SourceURL:        "file://migrations",
		MigrateTriggers: map[string]func(response migrate.TriggerResponse) error{
			migrate.TrigRunMigrationVersionPre:  app.RunMigrationVersionPre,
			migrate.TrigRunMigrationVersionPost: app.RunMigrationVersionPost,
		},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = m.Up()
	//err = m.Down()
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
}

func (app *App) MigrationHistoryTable(response interface{}) error {
	r, _ := response.(mysql.TriggerResponse)
	fmt.Printf("Executing database trigger %s\n, %v\n", r.Trigger, r)
	query := "CREATE TABLE IF NOT EXISTS `" + app.MigrationTable + "_history` (`id` bigint not null primary key auto_increment, `version` bigint not null, `target` bigint, identifier varchar(255), `dirty` tinyint not null, migration text, `timestamp` datetime not null)"
	_, err := app.Connection.ExecContext(context.TODO(), query)
	return err
}

func (app *App) DatabaseRunPost(response interface{}) error {
	r, _ := response.(mysql.TriggerResponse)
	detail := r.Detail.(struct{ Query string })
	fmt.Printf("Executing database trigger %s\n, %v\n", r.Trigger, r.Detail)
	query := "UPDATE `" + app.MigrationTable + "_history` SET `migration` = ? WHERE `id` = ?"
	_, err := app.Connection.ExecContext(context.TODO(), query, detail.Query, app.HistoryID)
	if err != nil {
		fmt.Printf("Error updating migration history: %v\n", err)
		return err
	}

	return nil
}

func (app *App) RunMigrationVersionPre(r migrate.TriggerResponse) error {
	detail := r.Detail.(struct{ Migration *migrate.Migration })
	fmt.Printf("Executing migration trigger %s\n, %v\n", r.Trigger, r.Detail)
	query := "INSERT INTO `" + app.MigrationTable + "_history` (`version`, `identifier`, `target`, `dirty`, `timestamp`) VALUES (?, ?, ?, 1, NOW())"
	_, err := app.Connection.ExecContext(context.TODO(), query, detail.Migration.Version, detail.Migration.Identifier, detail.Migration.TargetVersion)
	if err != nil {
		fmt.Printf("Error inserting migration history: %v\n", err)
		return err
	}
	query = "SELECT LAST_INSERT_ID()"
	row := app.Connection.QueryRowContext(context.TODO(), query)
	return row.Scan(&app.HistoryID)
}

func (app *App) RunMigrationVersionPost(r migrate.TriggerResponse) error {
	fmt.Printf("Executing migration trigger %s\n, %v\n", r.Trigger, r.Detail)
	query := "UPDATE `" + app.MigrationTable + "_history` SET `dirty` = 0, `timestamp` = NOW() WHERE `id` = ?"
	_, err := app.Connection.ExecContext(context.TODO(), query, app.HistoryID)
	if err != nil {
		fmt.Printf("Error updating migration history: %v\n", err)
		return err
	}
	app.HistoryID = nil

	return nil
}
