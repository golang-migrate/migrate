FROM golang:1.11-alpine3.8 AS downloader
ARG VERSION

RUN apk add --no-cache git gcc musl-dev

WORKDIR /go/src/github.com/golang-migrate/migrate

COPY . ./

ENV GO111MODULE=on
ENV DATABASES="postgres mysql redshift cassandra spanner cockroachdb clickhouse"
ENV SOURCES="file go_bindata github aws_s3 google_cloud_storage"

RUN go build -a -o build/migrate.linux-386 -ldflags="-X main.Version=${VERSION}" -tags "$DATABASES $SOURCES" ./cli

FROM alpine:3.8

RUN apk add --no-cache ca-certificates

COPY --from=downloader /go/src/github.com/golang-migrate/migrate/build/migrate.linux-386 /migrate

ENTRYPOINT ["/migrate"]
CMD ["--help"]
