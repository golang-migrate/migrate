package gitlab

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	nurl "net/url"
	"os"
	"strconv"
	"strings"
)

import (
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/xanzy/go-gitlab"
)

func init() {
	source.Register("gitlab", &Gitlab{})
}

var (
	ErrNoUserInfo       = fmt.Errorf("no username:token provided")
	ErrNoAccessToken    = fmt.Errorf("no access token")
	ErrInvalidHost      = fmt.Errorf("invalid host")
	ErrInvalidProjectID = fmt.Errorf("invalid project id")
	ErrInvalidResponse  = fmt.Errorf("invalid response")
)

type Gitlab struct {
	client *gitlab.Client
	url    string

	projectID  string
	path       string
	options    *gitlab.ListTreeOptions
	migrations *source.Migrations
}

type Config struct {
}

func (g *Gitlab) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.User == nil {
		return nil, ErrNoUserInfo
	}

	password, ok := u.User.Password()
	if !ok {
		return nil, ErrNoAccessToken
	}

	gn := &Gitlab{
		client:     gitlab.NewClient(nil, password),
		url:        url,
		migrations: source.NewMigrations(),
	}

	if u.Host != "" {
		err = gn.client.SetBaseURL(u.Host)
		if err != nil {
			return nil, ErrInvalidHost
		}
	}

	pe := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pe) < 1 {
		return nil, ErrInvalidProjectID
	}
	gn.projectID = pe[0]
	if len(pe) > 1 {
		gn.path = strings.Join(pe[1:], "/")
	}

	gn.options = &gitlab.ListTreeOptions{
		Path: &gn.path,
		Ref:  &u.Fragment,
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
	nodes, response, err := g.client.Repositories.ListTree(g.projectID, g.options)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		return ErrInvalidResponse
	}

	for i := range nodes {
		m, err := g.nodeToMigration(nodes[i])
		if err != nil {
			continue
		}

		if !g.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", nodes[i].Name)
		}
	}

	return nil
}

func (g *Gitlab) nodeToMigration(node *gitlab.TreeNode) (*source.Migration, error) {
	m := source.Regex.FindStringSubmatch(node.Name)
	if len(m) == 5 {
		versionUint64, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, err
		}
		return &source.Migration{
			Version:    uint(versionUint64),
			Identifier: node.ID,
			Direction:  source.Direction(m[3]),
			Raw:        node.Name,
		}, nil
	}
	return nil, source.ErrParse
}

func (g *Gitlab) Close() error {
	return nil
}

func (g *Gitlab) First() (version uint, er error) {
	if v, ok := g.migrations.First(); !ok {
		return 0, &os.PathError{"first", g.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := g.migrations.Prev(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("prev for version %v", version), g.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) Next(version uint) (nextVersion uint, err error) {
	if v, ok := g.migrations.Next(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("next for version %v", version), g.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.migrations.Up(version); ok {
		f, response, err := g.client.RepositoryFiles.GetFile(m.Identifier, m.Raw, nil)
		if err != nil {
			return nil, "", err
		}

		if response.StatusCode != http.StatusOK {
			return nil, "", ErrInvalidResponse
		}

		return ioutil.NopCloser(strings.NewReader(f.Content)), m.Identifier, nil
	}

	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), g.path, os.ErrNotExist}
}

func (g *Gitlab) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.migrations.Down(version); ok {
		f, response, err := g.client.RepositoryFiles.GetFile(m.Identifier, m.Raw, nil)
		if err != nil {
			return nil, "", err
		}

		if response.StatusCode != http.StatusOK {
			return nil, "", ErrInvalidResponse
		}

		return ioutil.NopCloser(strings.NewReader(f.Content)), m.Identifier, nil
	}

	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), g.path, os.ErrNotExist}
}
