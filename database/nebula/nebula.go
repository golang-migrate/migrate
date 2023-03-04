package nebula

import (
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	nebula "github.com/vesoft-inc/nebula-go/v3"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

const (
	vidColumnName              = "Vid Type"
	spaceTypePrefixINT         = "INT"
	spaceTypePrefixFixedString = "FIXED_STRING"

	columnVersion          = "Version"
	columnDirty            = "Dirty"
	columnTagEdgeIndexName = "Index Name"
	columnTagEdgeName      = "Name"
)

var (
	DefaultMigrationsTag = "schema_migrations"
	DefaultTimeout       = 10

	ErrNilConfig = fmt.Errorf("no config")
)

// ModelRepo is an instance of Model Repository interface type
type Nebula struct {
	pool   *nebula.SessionPool
	config *Config

	spaceTypePrefix string
}

// NebulaConfig describes Nebula DB config
type Config struct {
	Address         string
	Port            int
	Username        string
	Password        string
	MigrationsSpace string
	MigrationsTag   string
	Timeout         int
}

func init() {
	database.Register("nebula", &Nebula{})
}

func WithInstance(pool *nebula.SessionPool, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	nb := &Nebula{
		pool:   pool,
		config: config,
	}

	if err := nb.init(); err != nil {
		return nil, err
	}

	return nb, nil
}

func checkResultSet(prefix string, res *nebula.ResultSet) error {
	if !res.IsSucceed() {
		return fmt.Errorf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, res.GetErrorCode(), res.GetErrorMsg())
	}
	return nil
}

func (nb *Nebula) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "tcp"

	// get hostname from URL
	nbHost := purl.Hostname()
	if len(nbHost) == 0 {
		return nil, fmt.Errorf("hostname can't be empty")
	}

	// get port from URL
	nbPortString := purl.Port()
	if len(nbPortString) == 0 {
		return nil, fmt.Errorf("port can't be empty")
	}
	nbPort, err := strconv.Atoi(nbPortString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse port: %w", err)
	}

	// get username from URL
	nbUsername := purl.Query().Get("username")
	if len(nbUsername) == 0 {
		return nil, fmt.Errorf("username can't be empty")
	}

	// get password from URL
	nbPassword := purl.Query().Get("password")
	if len(nbPassword) == 0 {
		return nil, fmt.Errorf("password can't be empty")
	}

	// get space from URL
	nbSpace := purl.Query().Get("space")
	if len(nbSpace) == 0 {
		return nil, fmt.Errorf("space can't be empty")
	}

	timeout := DefaultTimeout
	if s := purl.Query().Get("x-timeout"); s != "" {
		timeout, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	hostAddress := nebula.HostAddress{Host: nbHost, Port: nbPort}
	hostList := []nebula.HostAddress{hostAddress}

	// Initialize session pool
	sessionPoolConfig, err := nebula.NewSessionPoolConf(
		nbUsername,
		nbPassword,
		hostList,
		nbSpace,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create config for session pool: %w", err)
	}
	pool, err := nebula.NewSessionPool(*sessionPoolConfig, nebula.DefaultLogger{})
	if err != nil {
		return nil, fmt.Errorf("failed to create new session pool: %w", err)
	}

	nb = &Nebula{
		pool: pool,
		config: &Config{
			Address:         nbHost,
			Port:            nbPort,
			Username:        nbUsername,
			Password:        nbPassword,
			MigrationsSpace: nbSpace,
			MigrationsTag:   purl.Query().Get("x-migrations-tag"),
			Timeout:         timeout,
		},
	}

	if err := nb.init(); err != nil {
		return nil, err
	}

	return nb, nil
}

func (nb *Nebula) init() error {

	if len(nb.config.MigrationsTag) == 0 {
		nb.config.MigrationsTag = DefaultMigrationsTag
	}

	// We need to find out the format of VID in the space
	queryDescribe := fmt.Sprintf("DESCRIBE SPACE %s", nb.config.MigrationsSpace)
	resultSet, err := nb.pool.Execute(queryDescribe)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(queryDescribe)}
	}
	err = checkResultSet(queryDescribe, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(queryDescribe)}
	}
	rows, err := resultSet.GetValuesByColName(vidColumnName)
	if err != nil {
		return fmt.Errorf("failed to get vid column value: %w", err)
	}
	vidType, _ := rows[0].AsString()
	if strings.HasPrefix(vidType, spaceTypePrefixINT) {
		nb.spaceTypePrefix = spaceTypePrefixINT
	}
	if strings.HasPrefix(vidType, spaceTypePrefixFixedString) {
		nb.spaceTypePrefix = spaceTypePrefixFixedString
	}

	return nb.ensureVersionTag()
}

func (nb *Nebula) Run(r io.Reader) error {
	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	resultSet, err := nb.pool.Execute(string(migration))
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: migration}
	}
	err = checkResultSet(string(migration), resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: migration}
	}

	return nil
}

func (nb *Nebula) Version() (int, bool, error) {

	var (
		version int64
		dirty   int64
		query   = fmt.Sprintf(`
			USE %s;
			MATCH (v:%s)
			WITH v LIMIT 1000000
			RETURN v.schema_migrations.version AS %s,
				v.schema_migrations.dirty AS %s,
				v.schema_migrations.sequence AS Sequence
			ORDER BY Sequence DESC LIMIT 1`, nb.config.MigrationsSpace, nb.config.MigrationsTag, columnVersion, columnDirty)
	)

	resultSet, err := nb.pool.Execute(query)
	if err != nil {
		return 0, false, database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(query)}
	}
	err = checkResultSet(query, resultSet)
	if err != nil {
		return 0, false, database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(query)}
	}

	if resultSet.GetRowSize() == 0 {
		return database.NilVersion, false, nil
	}
	row, err := resultSet.GetRowValuesByIndex(0)
	if err != nil {
		return 0, false, err
	}
	rawVersion, err := row.GetValueByColName(columnVersion)
	if err != nil {
		return 0, false, err
	}
	version, _ = rawVersion.AsInt()

	rawDirty, err := row.GetValueByColName(columnDirty)
	if err != nil {
		return 0, false, err
	}
	dirty, _ = rawDirty.AsInt()

	return int(version), dirty == 1, nil
}

func (nb *Nebula) SetVersion(version int, dirty bool) error {

	var (
		bool = func(v bool) uint8 {
			if v {
				return 1
			}
			return 0
		}
		query = fmt.Sprintf(`
			USE %s;
			INSERT VERTEX IF NOT EXISTS %s(version, dirty, sequence) VALUES`,
			nb.config.MigrationsSpace, nb.config.MigrationsTag)
		value string
	)

	switch nb.spaceTypePrefix {
	case spaceTypePrefixINT:
		value = fmt.Sprintf(` %d:(%d, %d, %d)`,
			time.Now().UnixMicro(),
			version,
			bool(dirty),
			time.Now().UnixMicro(),
		)
	case nb.spaceTypePrefix:
		value = fmt.Sprintf(` '%s':(%d, %d, %d)`,
			strconv.FormatInt(time.Now().UnixMicro(), 10),
			version,
			bool(dirty),
			time.Now().UnixMicro(),
		)
	default:
		return fmt.Errorf("received unknown Space type")
	}
	resQuery := query + value

	resultSet, err := nb.pool.Execute(resQuery)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(resQuery)}
	}
	err = checkResultSet(resQuery, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(resQuery)}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (nb *Nebula) ensureVersionTag() (err error) {

	query := fmt.Sprintf("USE %s; DESCRIBE TAG %s;",
		nb.config.MigrationsSpace, nb.config.MigrationsTag)

	// check if migration table exists
	resultSet, err := nb.pool.Execute(query)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(query)}
	}
	err = checkResultSet(query, resultSet)
	if err != nil {
		if resultSet.GetErrorCode() != nebula.ErrorCode_E_EXECUTION_ERROR {
			return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(query)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	query = fmt.Sprintf(`
		USE %s;
		CREATE TAG %s(
			version int64,
			dirty int8,
			sequence int64
		);`, nb.config.MigrationsSpace, nb.config.MigrationsTag)

	resultSet, err = nb.pool.Execute(query)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(query)}
	}
	err = checkResultSet(query, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(query)}
	}

	// Tag in empty Space is not created immediately
	// In order to proceed without errors - wait for its creation
	time.Sleep(time.Duration(nb.config.Timeout) * time.Second)

	return nil
}

func (nb *Nebula) Drop() error {
	var (
		resultSet *nebula.ResultSet
		err       error

		resultedQuery string

		queryTagIndex     = "SHOW TAG INDEXES;"
		queryDropTagIndex = "DROP TAG INDEX %s;"

		queryTag     = "SHOW TAGS;"
		queryDropTag = "DROP TAG %s;"

		queryEdgeIndex     = "SHOW EDGE INDEXES;"
		queryDropEdgeIndex = "DROP EDGE INDEX %s;"

		queryEdge     = "SHOW EDGES;"
		queryDropEdge = "DROP EDGE %s;"
	)

	// To correctly drop all TAGs/EDGEs
	// We need to DROP them in a proper order
	// Firstly DROP all INDEXES (TAGs/EDGEs)
	// Afterwards DROP all TAGs/EDGEs

	// Get TAG INDEXES and form DROP query
	resultSet, err = nb.pool.Execute(queryTagIndex)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(queryTagIndex)}
	}
	err = checkResultSet(queryTagIndex, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(queryTagIndex)}
	}
	rawTagIndexes, _ := resultSet.GetValuesByColName(columnTagEdgeIndexName)
	for _, rawTagIndex := range rawTagIndexes {
		tagIndex, _ := rawTagIndex.AsString()
		resultedQuery += fmt.Sprintf(queryDropTagIndex, tagIndex)
	}

	// Get EDGE INDEXES and form DROP query
	resultSet, err = nb.pool.Execute(queryEdgeIndex)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(queryEdgeIndex)}
	}
	err = checkResultSet(queryEdgeIndex, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(queryEdgeIndex)}
	}
	rawEdgeIndexes, _ := resultSet.GetValuesByColName(columnTagEdgeIndexName)
	for _, rawEdgeIndex := range rawEdgeIndexes {
		edgeIndex, _ := rawEdgeIndex.AsString()
		resultedQuery += fmt.Sprintf(queryDropEdgeIndex, edgeIndex)
	}

	// Get TAGS and form DROP query
	resultSet, err = nb.pool.Execute(queryTag)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(queryTag)}
	}
	err = checkResultSet(queryTag, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(queryTag)}
	}
	rawTags, _ := resultSet.GetValuesByColName(columnTagEdgeName)
	for _, rawTag := range rawTags {
		tag, _ := rawTag.AsString()
		resultedQuery += fmt.Sprintf(queryDropTag, tag)
	}

	// Get EDGE and form DROP query
	resultSet, err = nb.pool.Execute(queryEdge)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(queryEdge)}
	}
	err = checkResultSet(queryEdge, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(queryEdge)}
	}
	rawEdges, _ := resultSet.GetValuesByColName(columnTagEdgeName)
	for _, rawEdge := range rawEdges {
		edge, _ := rawEdge.AsString()
		resultedQuery += fmt.Sprintf(queryDropEdge, edge)
	}

	// Now DROP everything
	resultSet, err = nb.pool.Execute(resultedQuery)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed query execution", Query: []byte(resultedQuery)}
	}
	err = checkResultSet(resultedQuery, resultSet)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed checking query results", Query: []byte(resultedQuery)}
	}

	return nil
}

// Nebula doesn't support database locking
func (nb *Nebula) Lock() error {
	return nil
}

// Nebula doesn't support database locking
func (nb *Nebula) Unlock() error {
	return nil
}

func (nb *Nebula) Close() error {
	nb.pool.Close()
	return nil
}
