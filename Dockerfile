FROM golang:1.10-alpine3.7 AS downloader
ARG VERSION

RUN apk add --no-cache git gcc musl-dev

WORKDIR /go/src/github.com/golang-migrate/migrate

ENV DATABASES="postgres mysql redshift cassandra spanner cockroachdb clickhouse"
ENV SOURCES="file go-bindata github aws-s3 google-cloud-storage"

COPY *.go ./
COPY cli ./cli
COPY database ./database
COPY source ./source

RUN go get -v ./... && \
    go get -u github.com/fsouza/fake-gcs-server/fakestorage && \
    go get -u github.com/kshvakov/clickhouse && \
    go build -a -o build/migrate.linux-386 -ldflags="-X main.Version=${VERSION}" -tags "$DATABASES $SOURCES" ./cli

FROM alpine:3.7

RUN apk add --no-cache ca-certificates

COPY --from=downloader /go/src/github.com/golang-migrate/migrate/build/migrate.linux-386 /migrate
RUN chmod u+x /migrate

ENTRYPOINT ["/migrate"]
CMD ["--help"]