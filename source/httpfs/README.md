# httpfs

## Usage

To create migration data source from `http.FileSystem` instance use
`Init()` or `New()` functions. Users of this package are responsible for
getting `http.FileSystem` instance. It is not possible to create httpfs instance
from URL.

Example of using `http.Dir()` to read migrations from `sql` directory:

```go
	var d httpfs.Driver
	if err := d.Init(http.Dir("sql"), ""); err != nil {
		// do something
	}
	m, err := migrate.NewWithSourceInstance("httpfs", src, "database://url")
	if err != nil {
		// do something
	}
        err = m.Up()
	...
```

Using `New()` instead of `WithInstance()` reduces the number of errors that
needs to be handled, example:

```go
	m, err := migrate.NewWithSourceInstance("httpfs", httpfs.New(http.Dir("sql"), ""), "database://url")
	if err != nil {
		// do something
	}
        err = m.Up()
	...
```

