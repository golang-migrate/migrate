package googlecloudstorage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/golang-migrate/migrate/v4/source"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func init() {
	source.Register("gcs", &gcs{})
}

type gcs struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	prefix     string
	migrations *source.Migrations
}

func (g *gcs) Open(ctx context.Context, folder string) (source.Driver, error) {
	u, err := url.Parse(folder)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx, option.WithGRPCDialOption(grpc.WithStatsHandler(otelgrpc.NewClientHandler())))
	if err != nil {
		return nil, err
	}
	driver := gcs{
		client:     client,
		bucket:     client.Bucket(u.Host),
		prefix:     strings.Trim(u.Path, "/") + "/",
		migrations: source.NewMigrations(),
	}
	if err = driver.loadMigrations(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &driver, nil
}

func (g *gcs) loadMigrations(ctx context.Context) error {
	iter := g.bucket.Objects(ctx, &storage.Query{
		Prefix:    g.prefix,
		Delimiter: "/",
	})
	object, err := iter.Next()
	for ; err == nil; object, err = iter.Next() {
		_, fileName := path.Split(object.Name)
		m, parseErr := source.DefaultParse(fileName)
		if parseErr != nil {
			continue
		}
		if !g.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", object.Name)
		}
	}
	if err != iterator.Done {
		return err
	}
	return nil
}

func (g *gcs) Close(ctx context.Context) error {
	return g.client.Close()
}

func (g *gcs) First(ctx context.Context) (uint, error) {
	v, ok := g.migrations.First(ctx)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (g *gcs) Prev(ctx context.Context, version uint) (uint, error) {
	v, ok := g.migrations.Prev(ctx, version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (g *gcs) Next(ctx context.Context, version uint) (uint, error) {
	v, ok := g.migrations.Next(ctx, version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (g *gcs) ReadUp(ctx context.Context, version uint) (io.ReadCloser, string, error) {
	if m, ok := g.migrations.Up(ctx, version); ok {
		return g.open(ctx, m)
	}
	return nil, "", os.ErrNotExist
}

func (g *gcs) ReadDown(ctx context.Context, version uint) (io.ReadCloser, string, error) {
	if m, ok := g.migrations.Down(ctx, version); ok {
		return g.open(ctx, m)
	}
	return nil, "", os.ErrNotExist
}

func (g *gcs) open(ctx context.Context, m *source.Migration) (io.ReadCloser, string, error) {
	objectPath := path.Join(g.prefix, m.Raw)
	reader, err := g.bucket.Object(objectPath).NewReader(ctx)
	if err != nil {
		return nil, "", err
	}
	return reader, m.Identifier, nil
}
