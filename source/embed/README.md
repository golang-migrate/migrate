# embed

```golang
//go:embed *.sql
var MigrationFiles embed.FS 

```

```golang
embed, err := migrations.NewEmbed(migrations.MigrationFiles, ".")
if err != nil {
    klog.Error(fmt.Sprintf("newConnectionEngine migrations.NewEmbed error:%v", err))
    return
}
m, err := migrate.NewWithInstance(
    "embed",
    embed,
    "mysql",
    dbdriver,
)
```