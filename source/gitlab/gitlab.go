package gitlab

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	nurl "net/url"
	"os"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/xanzy/go-gitlab"
)

func init() {
	source.Register("gitlab", &Gitlab{})
}

const DefaultMaxItemsPerPage = 100

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

	projectID   string
	path        string
	listOptions *gitlab.ListTreeOptions
	getOptions  *gitlab.GetFileOptions
	migrations  *source.Migrations
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
		uri := nurl.URL{
			Scheme: "https",
			Host:   u.Host,
		}

		err = gn.client.SetBaseURL(uri.String())
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

	gn.listOptions = &gitlab.ListTreeOptions{
		Path: &gn.path,
		Ref:  &u.Fragment,
		ListOptions: gitlab.ListOptions{
			PerPage: DefaultMaxItemsPerPage,
		},
	}

	gn.getOptions = &gitlab.GetFileOptions{
		Ref: &u.Fragment,
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
	var nodes []*gitlab.TreeNode
	for {
		n, response, err := g.client.Repositories.ListTree(g.projectID, g.listOptions)
		if err != nil {
			return err
		}

		if response.StatusCode != http.StatusOK {
			return ErrInvalidResponse
		}

		nodes = append(nodes, n...)
		if response.CurrentPage >= response.TotalPages {
			break
		}
		g.listOptions.ListOptions.Page = response.NextPage
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
			Identifier: m[2],
			Direction:  source.Direction(m[3]),
			Raw:        g.path + "/" + node.Name,
		}, nil
	}
	return nil, source.ErrParse
}

func (g *Gitlab) Close() error {
	return nil
}

func (g *Gitlab) First() (version uint, er error) {
	if v, ok := g.migrations.First(); !ok {
		return 0, &os.PathError{Op: "first", Path: g.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := g.migrations.Prev(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: g.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) Next(version uint) (nextVersion uint, err error) {
	if v, ok := g.migrations.Next(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: g.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (g *Gitlab) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.migrations.Up(version); ok {
		f, response, err := g.client.RepositoryFiles.GetFile(g.projectID, m.Raw, g.getOptions)
		if err != nil {
			return nil, "", err
		}

		if response.StatusCode != http.StatusOK {
			return nil, "", ErrInvalidResponse
		}

		content, err := base64.StdEncoding.DecodeString(f.Content)
		if err != nil {
			return nil, "", err
		}

		return io.NopCloser(strings.NewReader(string(content))), m.Identifier, nil
	}

	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: g.path, Err: os.ErrNotExist}
}

func (g *Gitlab) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.migrations.Down(version); ok {
		f, response, err := g.client.RepositoryFiles.GetFile(g.projectID, m.Raw, g.getOptions)
		if err != nil {
			return nil, "", err
		}

		if response.StatusCode != http.StatusOK {
			return nil, "", ErrInvalidResponse
		}

		content, err := base64.StdEncoding.DecodeString(f.Content)
		if err != nil {
			return nil, "", err
		}

		return io.NopCloser(strings.NewReader(string(content))), m.Identifier, nil
	}

	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: g.path, Err: os.ErrNotExist}
}
