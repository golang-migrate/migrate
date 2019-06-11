package github

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"
	"path"
	"strings"
)

import (
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/google/go-github/github"
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
	Client *github.Client
	URL    string

	PathOwner  string
	PathRepo   string
	Path       string
	Options    *github.RepositoryContentGetOptions
	Migrations *source.Migrations
}

type Config struct {
}

func (g *Github) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.User == nil {
		return nil, ErrNoUserInfo
	}

	password, ok := u.User.Password()
	if !ok {
		return nil, ErrNoUserInfo
	}

	tr := &github.BasicAuthTransport{
		Username: u.User.Username(),
		Password: password,
	}

	gn := &Github{
		Client:     github.NewClient(tr.Client()),
		URL:        url,
		Migrations: source.NewMigrations(),
		Options:    &github.RepositoryContentGetOptions{Ref: u.Fragment},
	}

	// set owner, repo and path in repo
	gn.PathOwner = u.Host
	pe := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pe) < 1 {
		return nil, ErrInvalidRepo
	}
	gn.PathRepo = pe[0]
	if len(pe) > 1 {
		gn.Path = strings.Join(pe[1:], "/")
	}

	if err := gn.ReadDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

func WithInstance(client *github.Client, config *Config) (source.Driver, error) {
	gn := &Github{
		Client:     client,
		Migrations: source.NewMigrations(),
	}
	if err := gn.ReadDirectory(); err != nil {
		return nil, err
	}
	return gn, nil
}

func (g *Github) ReadDirectory() error {
	fileContent, dirContents, _, err := g.Client.Repositories.GetContents(context.Background(), g.PathOwner, g.PathRepo, g.Path, g.Options)
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
		if !g.Migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", *fi.Name)
		}
	}

	return nil
}

func (g *Github) Close() error {
	return nil
}

func (g *Github) First() (version uint, er error) {
	if v, ok := g.Migrations.First(); !ok {
		return 0, &os.PathError{Op: "first", Path: g.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Github) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := g.Migrations.Prev(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: g.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Github) Next(version uint) (nextVersion uint, err error) {
	if v, ok := g.Migrations.Next(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: g.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Github) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.Migrations.Up(version); ok {
		file, _, _, err := g.Client.Repositories.GetContents(context.Background(), g.PathOwner, g.PathRepo, path.Join(g.Path, m.Raw), g.Options)
		if err != nil {
			return nil, "", err
		}
		if file != nil {
			r, err := file.GetContent()
			if err != nil {
				return nil, "", err
			}
			return ioutil.NopCloser(strings.NewReader(r)), m.Identifier, nil
		}
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: g.Path, Err: os.ErrNotExist}
}

func (g *Github) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.Migrations.Down(version); ok {
		file, _, _, err := g.Client.Repositories.GetContents(context.Background(), g.PathOwner, g.PathRepo, path.Join(g.Path, m.Raw), g.Options)
		if err != nil {
			return nil, "", err
		}
		if file != nil {
			r, err := file.GetContent()
			if err != nil {
				return nil, "", err
			}
			return ioutil.NopCloser(strings.NewReader(r)), m.Identifier, nil
		}
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: g.Path, Err: os.ErrNotExist}
}
