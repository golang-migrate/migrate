package github_ee

import (
	"crypto/tls"
	"fmt"
	"net/http"
	nurl "net/url"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
	gh "github.com/golang-migrate/migrate/v4/source/github"

	"github.com/google/go-github/v39/github"
)

func init() {
	source.Register("github-ee", &GithubEE{})
}

type GithubEE struct {
	source.Driver
}

func (g *GithubEE) Open(url string) (source.Driver, error) {
	verifyTLS := true

	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if o := u.Query().Get("verify-tls"); o != "" {
		verifyTLS = parseBool(o, verifyTLS)
	}

	if u.User == nil {
		return nil, gh.ErrNoUserInfo
	}

	password, ok := u.User.Password()
	if !ok {
		return nil, gh.ErrNoUserInfo
	}

	ghc, err := g.createGithubClient(u.Host, u.User.Username(), password, verifyTLS)
	if err != nil {
		return nil, err
	}

	pe := strings.Split(strings.Trim(u.Path, "/"), "/")

	if len(pe) < 1 {
		return nil, gh.ErrInvalidRepo
	}

	cfg := &gh.Config{
		Owner: pe[0],
		Repo:  pe[1],
		Ref:   u.Fragment,
	}

	if len(pe) > 2 {
		cfg.Path = strings.Join(pe[2:], "/")
	}

	i, err := gh.WithInstance(ghc, cfg)
	if err != nil {
		return nil, err
	}

	return &GithubEE{Driver: i}, nil
}

func (g *GithubEE) createGithubClient(host, username, password string, verifyTLS bool) (*github.Client, error) {
	tr := &github.BasicAuthTransport{
		Username: username,
		Password: password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: !verifyTLS},
		},
	}

	apiHost := fmt.Sprintf("https://%s/api/v3", host)
	uploadHost := fmt.Sprintf("https://uploads.%s", host)

	return github.NewEnterpriseClient(apiHost, uploadHost, tr.Client())
}

func parseBool(val string, fallback bool) bool {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return fallback
	}

	return b
}
