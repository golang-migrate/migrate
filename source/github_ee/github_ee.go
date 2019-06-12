package github_ee

import (
	"fmt"
	nurl "net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
	gh "github.com/golang-migrate/migrate/v4/source/github"
	"github.com/google/go-github/github"
)

func init() {
	source.Register("github-ee", &GithubEE{})
}

type GithubEE struct {
	gh.Github
}

func (g *GithubEE) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.User == nil {
		return nil, gh.ErrNoUserInfo
	}

	password, ok := u.User.Password()
	if !ok {
		return nil, gh.ErrNoUserInfo
	}

	ghc, err := g.createGithubClient(u.Host, u.User.Username(), password)
	if err != nil {
		return nil, err
	}

	gn := &GithubEE{
		Github: gh.Github{
			Client:     ghc,
			URL:        url,
			Migrations: source.NewMigrations(),
			Options:    &github.RepositoryContentGetOptions{Ref: u.Fragment},
		},
	}

	pe := strings.Split(strings.Trim(u.Path, "/"), "/")

	if len(pe) < 1 {
		return nil, gh.ErrInvalidRepo
	}

	gn.PathOwner = pe[0]
	gn.PathRepo = pe[1]

	if len(pe) > 2 {
		gn.Path = strings.Join(pe[2:], "/")
	}

	if err := gn.ReadDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

func (g *GithubEE) createGithubClient(host, username, password string) (*github.Client, error) {
	tr := &github.BasicAuthTransport{
		Username: username,
		Password: password,
	}

	apiHost := fmt.Sprintf("https://%s/api/v3", host)
	uploadHost := fmt.Sprintf("https://uploads.%s", host)

	return github.NewEnterpriseClient(apiHost, uploadHost, tr.Client())
}
