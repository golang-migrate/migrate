FROM alpine:3.13

RUN apk add --no-cache ca-certificates

ENTRYPOINT ["/usr/bin/migrate"]
CMD ["--help"]

COPY migrate /usr/bin/migrate