package bindata

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/golang-migrate/migrate/v4/source"
)

type AssetFunc func(name string) ([]byte, error)

func Resource(names []string, afn AssetFunc) *AssetSource {
	return &AssetSource{
		Names:     names,
		AssetFunc: afn,
	}
}

type AssetSource struct {
	Names     []string
	AssetFunc AssetFunc
}

func init() {
	source.Register("go-bindata", &Bindata{})
}

type Bindata struct {
	path        string
	assetSource *AssetSource
	migrations  *source.Migrations
}

func (b *Bindata) Open(ctx context.Context, url string) (source.Driver, error) {
	return nil, fmt.Errorf("not yet implemented")
}

var (
	ErrNoAssetSource = fmt.Errorf("expects *AssetSource")
)

func WithInstance(ctx context.Context, instance interface{}) (source.Driver, error) {
	if _, ok := instance.(*AssetSource); !ok {
		return nil, ErrNoAssetSource
	}
	as := instance.(*AssetSource)

	bn := &Bindata{
		path:        "<go-bindata>",
		assetSource: as,
		migrations:  source.NewMigrations(),
	}

	for _, fi := range as.Names {
		m, err := source.DefaultParse(fi)
		if err != nil {
			continue // ignore files that we can't parse
		}

		if !bn.migrations.Append(m) {
			return nil, fmt.Errorf("unable to parse file %v", fi)
		}
	}

	return bn, nil
}

func (b *Bindata) Close(ctx context.Context) error {
	return nil
}

func (b *Bindata) First(ctx context.Context) (version uint, err error) {
	if v, ok := b.migrations.First(ctx); !ok {
		return 0, &os.PathError{Op: "first", Path: b.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Bindata) Prev(ctx context.Context, version uint) (prevVersion uint, err error) {
	if v, ok := b.migrations.Prev(ctx, version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: b.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Bindata) Next(ctx context.Context, version uint) (nextVersion uint, err error) {
	if v, ok := b.migrations.Next(ctx, version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: b.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Bindata) ReadUp(ctx context.Context, version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := b.migrations.Up(version); ok {
		body, err := b.assetSource.AssetFunc(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return io.NopCloser(bytes.NewReader(body)), m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: b.path, Err: os.ErrNotExist}
}

func (b *Bindata) ReadDown(ctx context.Context, version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := b.migrations.Down(version); ok {
		body, err := b.assetSource.AssetFunc(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return io.NopCloser(bytes.NewReader(body)), m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: b.path, Err: os.ErrNotExist}
}
