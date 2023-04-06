package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	nurl "net/url"
	"os"
	"path"
	"strings"

	"golang.org/x/oauth2"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/google/go-github/v39/github"
)

func init() {
	source.Register("github", &Github{})
}

var (
	ErrNoUserInfo          = fmt.Errorf("no username:token provided")
	ErrNoAccessToken       = fmt.Errorf("no access token")
	ErrInvalidRepo         = fmt.Errorf("invalid repo")
	ErrInvalidGithubClient = fmt.Errorf("expected *github.Client")
	ErrNoDir               = fmt.Errorf("no directory")
)

type Github struct {
	config     *Config
	client     *github.Client
	options    *github.RepositoryContentGetOptions
	migrations *source.Migrations
}

type Config struct {
	Owner string
	Repo  string
	Path  string
	Ref   string
}

func (g *Github) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	// client defaults to http.DefaultClient
	var client *http.Client
	if u.User != nil {
		password, ok := u.User.Password()
		if !ok {
			return nil, ErrNoUserInfo
		}
		ts := oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: password},
		)
		client = oauth2.NewClient(context.Background(), ts)

	}

	gn := &Github{
		client:     github.NewClient(client),
		migrations: source.NewMigrations(),
		options:    &github.RepositoryContentGetOptions{Ref: u.Fragment},
	}

	gn.ensureFields()

	// set owner, repo and path in repo
	gn.config.Owner = u.Host
	pe := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pe) < 1 {
		return nil, ErrInvalidRepo
	}
	gn.config.Repo = pe[0]
	if len(pe) > 1 {
		gn.config.Path = strings.Join(pe[1:], "/")
	}

	if err := gn.readDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

func WithInstance(client *github.Client, config *Config) (source.Driver, error) {
	gn := &Github{
		client:     client,
		config:     config,
		migrations: source.NewMigrations(),
		options:    &github.RepositoryContentGetOptions{Ref: config.Ref},
	}

	if err := gn.readDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

func (g *Github) readDirectory() error {
	g.ensureFields()

	fileContent, dirContents, _, err := g.client.Repositories.GetContents(
		context.Background(),
		g.config.Owner,
		g.config.Repo,
		g.config.Path,
		g.options,
	)

	if err != nil {
		return err
	}
	if fileContent != nil {
		return ErrNoDir
	}

	for _, fi := range dirContents {
		m, err := source.DefaultParse(*fi.Name)
		if err != nil {
			continue // ignore files that we can't parse
		}
		if !g.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", *fi.Name)
		}
	}

	return nil
}

func (g *Github) ensureFields() {
	if g.config == nil {
		g.config = &Config{}
	}
}

func (g *Github) Close() error {
	return nil
}

func (g *Github) First() (version uint, err error) {
	g.ensureFields()

	if v, ok := g.migrations.First(); !ok {
		return 0, &os.PathError{Op: "first", Path: g.config.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Github) Prev(version uint) (prevVersion uint, err error) {
	g.ensureFields()

	if v, ok := g.migrations.Prev(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: g.config.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Github) Next(version uint) (nextVersion uint, err error) {
	g.ensureFields()

	if v, ok := g.migrations.Next(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: g.config.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Github) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	g.ensureFields()

	if m, ok := g.migrations.Up(version); ok {
		r, _, err := g.client.Repositories.DownloadContents(
			context.Background(),
			g.config.Owner,
			g.config.Repo,
			path.Join(g.config.Path, m.Raw),
			g.options,
		)

		if err != nil {
			return nil, "", err
		}
		return r, m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: g.config.Path, Err: os.ErrNotExist}
}

func (g *Github) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	g.ensureFields()

	if m, ok := g.migrations.Down(version); ok {
		r, _, err := g.client.Repositories.DownloadContents(
			context.Background(),
			g.config.Owner,
			g.config.Repo,
			path.Join(g.config.Path, m.Raw),
			g.options,
		)

		if err != nil {
			return nil, "", err
		}
		return r, m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: g.config.Path, Err: os.ErrNotExist}
}
