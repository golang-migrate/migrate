package gitlab

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"
	"path"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/xanzy/go-gitlab"
)

func init() {
	source.Register("gitlab", &Gitlab{})
}

var (
	ErrNoAccessToken = fmt.Errorf("no access token")
	ErrInvalidRepo   = fmt.Errorf("invalid repo")
	ErrNoBranch      = fmt.Errorf("branch was not specified")
	ErrNoDir         = fmt.Errorf("no directory")
)

type Gitlab struct {
	client       *gitlab.Client
	branch       string
	pathRepo     string
	relativePath string
	migrations   *source.Migrations
}

type Config struct {
}

func (g *Gitlab) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.User == nil {
		return nil, ErrNoAccessToken
	}

	gn := &Gitlab{
		client:     gitlab.NewClient(nil, u.User.Username()),
		migrations: source.NewMigrations(),
	}

	gn.client.SetBaseURL(fmt.Sprintf("https://%s", u.Host))

	if len(u.Fragment) > 0 {
		gn.branch = u.Fragment
	} else {
		gn.branch = "master"
	}

	pathSplit := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pathSplit) < 1 {
		return nil, ErrInvalidRepo
	}

	gn.pathRepo = path.Join(pathSplit[0], pathSplit[1])
	if len(pathSplit) > 1 {
		gn.relativePath = strings.Join(pathSplit[2:], "/")
	}

	if err := gn.readDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

func WithInstance(client *gitlab.Client, config *Config) (source.Driver, error) {
	gn := &Gitlab{
		client:     client,
		migrations: source.NewMigrations(),
	}
	if err := gn.readDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

func (g *Gitlab) readDirectory() error {
	listTreeOptions := &gitlab.ListTreeOptions{Path: &g.relativePath}

	treeNodes, _, err := g.client.Repositories.ListTree(g.pathRepo, listTreeOptions)
	if err != nil {
		return err
	}
	if treeNodes == nil {
		return ErrNoDir
	}

	for _, fi := range treeNodes {
		expectedPath := path.Join(g.relativePath, fi.Name)
		if expectedPath != fi.Path {
			continue
		}
		m, err := source.DefaultParse(fi.Name)
		if err != nil {
			continue // ignore files that we can't parse
		}
		if !g.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", fi.Name)
		}
	}

	return nil
}

func (g *Gitlab) Close() error {
	return nil
}

func (g *Gitlab) First() (version uint, er error) {
	if v, ok := g.migrations.First(); !ok {
		return 0, &os.PathError{"first", g.relativePath, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := g.migrations.Prev(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("prev for version %v", version), g.relativePath, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) Next(version uint) (nextVersion uint, err error) {
	if v, ok := g.migrations.Next(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("next for version %v", version), g.relativePath, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	return g.read(version, g.migrations.Up)
}

func (g *Gitlab) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	return g.read(version, g.migrations.Down)
}

func (g *Gitlab) read(version uint, migrationAction func(version uint) (m *source.Migration, ok bool)) (r io.ReadCloser, identifier string, err error) {
	if migration, ok := migrationAction(version); ok {

		var getFileOptions = &gitlab.GetRawFileOptions{Ref: &g.branch}
		file, _, err := g.client.RepositoryFiles.GetRawFile(g.pathRepo, path.Join(g.relativePath, migration.Raw), getFileOptions)
		if err != nil {
			return nil, "", err
		}
		if file != nil {
			return ioutil.NopCloser(bytes.NewReader(file)), migration.Identifier, nil
		}
	}

	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), g.relativePath, err}
}
