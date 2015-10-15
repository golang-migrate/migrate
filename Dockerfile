FROM scratch
ADD migrate /migrate
ENTRYPOINT ["/migrate"]
