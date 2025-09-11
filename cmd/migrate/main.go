package main

import (
	"log"
	"strings"

	"github.com/golang-migrate/migrate/v4/internal/cli"
	"github.com/infobloxopen/hotload"
	_ "github.com/infobloxopen/hotload/fsnotify"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func init() {
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AddConfigPath(viper.GetString("config.source"))
	if viper.GetString("config.file") != "" {
		viper.SetConfigName(viper.GetString("config.file"))
		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("cannot load configuration: %v", err)
		}
	}
	// logrus formatter
	customFormatter := new(logrus.JSONFormatter)
	logrus.SetFormatter(customFormatter)

	hotload.RegisterSQLDriver("pgx", stdlib.GetDefaultDriver())
	hotload.RegisterSQLDriver("postgres", pq.Driver{})
	hotload.RegisterSQLDriver("postgresql", pq.Driver{})
}

func main() {
	cli.Main(Version)
}
