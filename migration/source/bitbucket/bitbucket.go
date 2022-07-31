package bitbucket

import (
	"fmt"
	"io"
	nurl "net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/ktrysmt/go-bitbucket"
)

func init() {
	source.Register("bitbucket", &Bitbucket{})
}

var (
	ErrNoUserInfo             = fmt.Errorf("no username:password provided")
	ErrNoAccessToken          = fmt.Errorf("no password/app password")
	ErrInvalidRepo            = fmt.Errorf("invalid repo")
	ErrInvalidBitbucketClient = fmt.Errorf("expected *bitbucket.Client")
	ErrNoDir                  = fmt.Errorf("no directory")
)

type Bitbucket struct {
	config     *Config
	client     *bitbucket.Client
	migrations *source.Migrations
}

type Config struct {
	Owner string
	Repo  string
	Path  string
	Ref   string
}

func (b *Bitbucket) Open(url string) (source.Driver, error) {
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

	cl := bitbucket.NewBasicAuth(u.User.Username(), password)

	cfg := &Config{}
	// set owner, repo and path in repo
	cfg.Owner = u.Host
	pe := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pe) < 1 {
		return nil, ErrInvalidRepo
	}
	cfg.Repo = pe[0]
	if len(pe) > 1 {
		cfg.Path = strings.Join(pe[1:], "/")
	}
	cfg.Ref = u.Fragment

	bi, err := WithInstance(cl, cfg)
	if err != nil {
		return nil, err
	}

	return bi, nil
}

func WithInstance(client *bitbucket.Client, config *Config) (source.Driver, error) {
	bi := &Bitbucket{
		client:     client,
		config:     config,
		migrations: source.NewMigrations(),
	}

	if err := bi.readDirectory(); err != nil {
		return nil, err
	}

	return bi, nil
}

func (b *Bitbucket) readDirectory() error {
	b.ensureFields()

	fOpt := &bitbucket.RepositoryFilesOptions{
		Owner:    b.config.Owner,
		RepoSlug: b.config.Repo,
		Ref:      b.config.Ref,
		Path:     b.config.Path,
	}

	dirContents, err := b.client.Repositories.Repository.ListFiles(fOpt)

	if err != nil {
		return err
	}

	for _, fi := range dirContents {

		m, err := source.DefaultParse(filepath.Base(fi.Path))
		if err != nil {
			continue // ignore files that we can't parse
		}
		if !b.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", fi.Path)
		}
	}

	return nil
}

func (b *Bitbucket) ensureFields() {
	if b.config == nil {
		b.config = &Config{}
	}
}

func (b *Bitbucket) Close() error {
	return nil
}

func (b *Bitbucket) First() (version uint, er error) {
	b.ensureFields()

	if v, ok := b.migrations.First(); !ok {
		return 0, &os.PathError{Op: "first", Path: b.config.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Bitbucket) Prev(version uint) (prevVersion uint, err error) {
	b.ensureFields()

	if v, ok := b.migrations.Prev(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: b.config.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Bitbucket) Next(version uint) (nextVersion uint, err error) {
	b.ensureFields()

	if v, ok := b.migrations.Next(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: b.config.Path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Bitbucket) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	b.ensureFields()

	if m, ok := b.migrations.Up(version); ok {
		fBlobOpt := &bitbucket.RepositoryBlobOptions{
			Owner:    b.config.Owner,
			RepoSlug: b.config.Repo,
			Ref:      b.config.Ref,
			Path:     path.Join(b.config.Path, m.Raw),
		}
		file, err := b.client.Repositories.Repository.GetFileBlob(fBlobOpt)
		if err != nil {
			return nil, "", err
		}
		if file != nil {
			r := file.Content
			return io.NopCloser(strings.NewReader(string(r))), m.Identifier, nil
		}
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: b.config.Path, Err: os.ErrNotExist}
}

func (b *Bitbucket) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	b.ensureFields()

	if m, ok := b.migrations.Down(version); ok {
		fBlobOpt := &bitbucket.RepositoryBlobOptions{
			Owner:    b.config.Owner,
			RepoSlug: b.config.Repo,
			Ref:      b.config.Ref,
			Path:     path.Join(b.config.Path, m.Raw),
		}
		file, err := b.client.Repositories.Repository.GetFileBlob(fBlobOpt)

		if err != nil {
			return nil, "", err
		}
		if file != nil {
			r := file.Content

			return io.NopCloser(strings.NewReader(string(r))), m.Identifier, nil
		}
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: b.config.Path, Err: os.ErrNotExist}
}
