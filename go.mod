module github.com/mrqzzz/migrate

require (
<<<<<<< HEAD
	cloud.google.com/go v0.34.0
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78
	github.com/Microsoft/go-winio v0.4.11
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5
	github.com/aws/aws-sdk-go v1.15.54
	github.com/cockroachdb/cockroach-go v0.0.0-20181001143604-e0a95dfd547c
	github.com/cznic/b v0.0.0-20180115125044-35e9bbe41f07
	github.com/cznic/fileutil v0.0.0-20180108211300-6a051e75936f
	github.com/cznic/golex v0.0.0-20170803123110-4ab7c5e190e4
	github.com/cznic/internal v0.0.0-20180608152220-f44710a21d00
	github.com/cznic/lldb v1.1.0
	github.com/cznic/mathutil v0.0.0-20180504122225-ca4c9f2c1369
	github.com/cznic/ql v1.2.0
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65
	github.com/cznic/strutil v0.0.0-20171016134553-529a34b1c186
	github.com/cznic/zappy v0.0.0-20160723133515-2533cb5b45cc
	github.com/dhui/dktest v0.3.0
	github.com/docker/docker v0.7.3-0.20190108045446-77df18c24acf
	github.com/docker/go-connections v0.4.0
	github.com/docker/go-units v0.3.3
	github.com/edsrzf/mmap-go v0.0.0-20170320065105-0bce6a688712
	github.com/fsouza/fake-gcs-server v1.3.0
	github.com/go-ini/ini v1.39.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/go-stack/stack v1.8.0
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/gogo/protobuf v1.2.0
	github.com/golang-migrate/migrate/v4 v4.2.4
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/go-querystring v1.0.0
	github.com/googleapis/gax-go v2.0.0+incompatible
	github.com/gorilla/context v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/errwrap v1.0.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/jackc/pgx v3.2.0+incompatible
	github.com/jmespath/go-jmespath v0.0.0-20180206201540-c2b33e8439af
	github.com/konsorten/go-windows-terminal-sequences v1.0.1
	github.com/kshvakov/clickhouse v1.3.4
	github.com/lib/pq v1.0.0
	github.com/mattn/go-sqlite3 v1.9.0
	github.com/mongodb/mongo-go-driver v0.1.0
	github.com/opencontainers/go-digest v1.0.0-rc1
	github.com/opencontainers/image-spec v1.0.1
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.3.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/xdg/stringprep v1.0.0
	go.opencensus.io v0.17.0
	golang.org/x/crypto v0.0.0-20190103213133-ff983b9c42bc
	golang.org/x/net v0.0.0-20190108225652-1e06a53dbb7e
	golang.org/x/oauth2 v0.0.0-20181203162652-d668ce993890
	golang.org/x/sync v0.0.0-20181221193216-37e7f081c4d4
	golang.org/x/sys v0.0.0-20190108104531-7fbe1cd0fcc2
	golang.org/x/text v0.3.0
	golang.org/x/tools v0.0.0-20190108222858-421f03a57a64
	google.golang.org/api v0.0.0-20181015145326-625cd1887957
	google.golang.org/appengine v1.4.0
	google.golang.org/genproto v0.0.0-20190108161440-ae2f86662275
	google.golang.org/grpc v1.17.0
	gopkg.in/inf.v0 v0.9.1
=======
	cloud.google.com/go v0.37.4
	github.com/aws/aws-sdk-go v1.17.7
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/cockroachdb/apd v1.1.0 // indirect
	github.com/cockroachdb/cockroach-go v0.0.0-20181001143604-e0a95dfd547c
	github.com/containerd/containerd v1.2.7 // indirect
	github.com/cznic/ql v1.2.0
	github.com/denisenkom/go-mssqldb v0.0.0-20190515213511-eb9f6a1743f3
	github.com/dhui/dktest v0.3.0
	github.com/docker/docker v0.7.3-0.20190817195342-4760db040282
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20190301043612-f6df8288f9b4
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/go-github v17.0.0+incompatible
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgx v3.2.0+incompatible // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kshvakov/clickhouse v1.3.5
	github.com/lib/pq v1.0.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/morikuni/aec v0.0.0-20170113033406-39771216ff4c // indirect
	github.com/nakagami/firebirdsql v0.0.0-20190310045651-3c02a58cfed8
	github.com/pkg/errors v0.8.1 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	github.com/sirupsen/logrus v1.4.1 // indirect
	github.com/stretchr/testify v1.3.0
	github.com/tidwall/pretty v0.0.0-20180105212114-65a9db5fad51 // indirect
	github.com/xanzy/go-gitlab v0.15.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	gitlab.com/nyarla/go-crypt v0.0.0-20160106005555-d9a5dc2b789b // indirect
	go.mongodb.org/mongo-driver v1.1.0
	golang.org/x/crypto v0.0.0-20190426145343-a29dc8fdc734 // indirect
	golang.org/x/net v0.0.0-20190424112056-4829fb13d2c6
	golang.org/x/oauth2 v0.0.0-20190402181905-9f3314589c9a // indirect
	golang.org/x/sys v0.0.0-20190426135247-a129542de9ae // indirect
	golang.org/x/text v0.3.2 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20190425222832-ad9eeb80039a
	google.golang.org/api v0.4.0
	google.golang.org/appengine v1.5.0 // indirect
	google.golang.org/genproto v0.0.0-20190425155659-357c62f0e4bb
	google.golang.org/grpc v1.20.1 // indirect
>>>>>>> master-ORIGINAL_UPSTREAM
)
