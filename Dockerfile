FROM golang:1.13-alpine3.11 AS downloader
ARG VERSION

RUN apk add --no-cache git gcc musl-dev

# dependencies for neo4j
RUN apk add --update --no-cache ca-certificates cmake make g++ openssl-libs-static openssl-dev git curl pkgconfig

# build seabolt for neo4j driver
RUN git clone -b 1.7 https://github.com/neo4j-drivers/seabolt.git /seabolt
WORKDIR /seabolt/build
RUN cmake -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_LIBDIR=lib .. && cmake --build . --target install

WORKDIR /go/src/github.com/golang-migrate/migrate

COPY . ./

ENV GO111MODULE=on
ENV DATABASES="postgres mysql redshift cassandra spanner cockroachdb clickhouse mongodb sqlserver firebird neo4j"
ENV SOURCES="file go_bindata github github_ee aws_s3 google_cloud_storage godoc_vfs gitlab"

RUN go build -a -o build/migrate.linux-386 -ldflags="-s -w -X main.Version=${VERSION}" -tags "$DATABASES $SOURCES" ./cmd/migrate

FROM alpine:3.11

RUN apk add --no-cache ca-certificates

COPY --from=downloader /go/src/github.com/golang-migrate/migrate/build/migrate.linux-386 /migrate

ENTRYPOINT ["/migrate"]
CMD ["--help"]
