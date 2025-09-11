FROM golang:1.24-alpine AS builder
ARG VERSION

RUN apk add --no-cache git gcc musl-dev make

WORKDIR /go/src/github.com/infobloxopen/migrate

ENV GO111MODULE=on

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN make build-docker

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /go/src/github.com/infobloxopen/migrate/cmd/migrate/config /cli/config/
COPY --from=builder /go/src/github.com/infobloxopen/migrate/build/migrate.linux-386 /migrate
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs/

ENTRYPOINT ["migrate"]
CMD ["--help"]
