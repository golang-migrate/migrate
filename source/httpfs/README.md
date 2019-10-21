# httpfs

## Usage

To create migration data source from `http.FileSystem` instance use
`WithInstance()` function. Users of this package are responsible for getting
`http.FileSystem` instance. It is not possible to create httpfs instance from
URL.

Example of using `http.Dir()` to read migrations from `sql` directory:

```go
	src, err := httpfs.WithInstance(http.Dir("sql"), "")
	m, err := migrate.NewWithSourceInstance("httpfs", src, "database://url")
        err = m.Up()
```
