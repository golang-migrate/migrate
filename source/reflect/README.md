# Reflect driver

This driver allows you to define your up/down statements in a struct (even an anonymous struct). The driver uses reflection to examine the fields of the struct and return the correct statements.

Struct fields must end with '_up' or '_down' and the driver pairs matching fields automatically. The order of execution is the same as the order of definition. There doesn't need to be a matching down for each up statement, orphaned down statements are ignored.

The migration version is calculated automatically by default but if you want to, you can specify it manually by adding a `migrate` tag to each field of the struct. See example #2.

## Example 1 - auto version

```
	migrations := &struct {
		Users_up   string
		Users_down string
		Folders_up string
		Posts_up   string
		Posts_down string
	}{

		Users_up: `
			create table users (
			id int not null primary key,
			created_at timestamp not null,
			email text not null,
			password text not null);`,

		Users_down: `drop table users;`,

		Folders_up: `create table folders (
			id int not null primary key,
			created_at timestamp not null,
			label text not null);`,

		Posts_up: `
			create table posts (
			id int not null primary key,
			created_at timestamp not null,
			user_id int not null,
			body text not null);`,

		Posts_down: `drop table posts;`,
	}

	driver, err := New(migrations)
	if err != nil {
		log.Fatal(err)
	}

	driver, err = driver.Open("reflect://")
	if err != nil {
		log.Fatal(err)
	}
  ...

```

## Example 2 - struct tags

```
	migrations := &struct {
		Table1_up   string `migrate:"1"`
		Table1_down string `migrate:"1"`
		Table2_up   string `migrate:"3"`
		Table3_up   string `migrate:"4"`
		Table3_down string `migrate:"4"`
		Table4_down string `migrate:"5"`
		Table5_down string `migrate:"7"`
		Table5_up   string `migrate:"7"`
	}{
		Table1_up:   `test statement 1`,
		Table1_down: `test statement 2`,
		Table2_up:   `test statement 3`,
		Table3_up:   `test statement 4`,
		Table3_down: `test statement 5`,
		Table4_down: `test statement 6`,
		Table5_up:   `test statement 7`,
		Table5_down: `test statement 8`,
	}
```