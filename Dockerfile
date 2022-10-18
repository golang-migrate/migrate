FROM golang:1.19-alpine3.16 AS envsetup
ARG VERSION

RUN apk add --no-cache git gcc musl-dev make
WORKDIR /go/src/github.com/golang-migrate/migrate
ENV GO111MODULE=on

FROM envsetup AS deps
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS builder
COPY . ./
RUN make build-docker

FROM alpine:3.16 as image
RUN apk add --no-cache ca-certificates

COPY --from=builder /go/src/github.com/golang-migrate/migrate/build/migrate.linux-386 /usr/local/bin/migrate
RUN ln -s /usr/local/bin/migrate /migrate

ENTRYPOINT ["migrate"]
CMD ["--help"]
