#!/usr/bin/env bash
set -euo pipefail

modules="$(go list -m -f '{{.Path}} {{.Version}}' all)"

selected_version() {
  awk -v module="$1" '$1 == module { print $2 }' <<<"$modules"
}

normalize_version() {
  local version="${1#v}"
  version="${version%%+*}"
  printf '%s\n' "$version"
}

version_ge() {
  local have want lowest
  have="$(normalize_version "$1")"
  want="$(normalize_version "$2")"
  lowest="$(printf '%s\n%s\n' "$want" "$have" | sort -V | head -n 1)"
  [[ "$lowest" == "$want" ]]
}

require_absent() {
  local module="$1"
  if [[ -n "$(selected_version "$module")" ]]; then
    printf 'vulnerable module selected: %s %s\n' "$module" "$(selected_version "$module")" >&2
    exit 1
  fi
}

require_min() {
  local module="$1"
  local minimum="$2"
  local version
  version="$(selected_version "$module")"
  if [[ -z "$version" ]]; then
    return
  fi
  if ! version_ge "$version" "$minimum"; then
    printf 'vulnerable module selected: %s %s < %s\n' "$module" "$version" "$minimum" >&2
    exit 1
  fi
}

require_otel_not_vulnerable() {
  local version
  version="$(selected_version go.opentelemetry.io/otel)"
  if [[ -n "$version" ]] && version_ge "$version" v1.36.0 && ! version_ge "$version" v1.41.0; then
    printf 'vulnerable module selected: go.opentelemetry.io/otel %s\n' "$version" >&2
    exit 1
  fi
}

require_absent github.com/docker/docker
require_min github.com/docker/cli v29.3.1
require_min github.com/gofiber/fiber/v2 v2.52.13
require_min github.com/opencontainers/runc v1.3.6
require_min go.mongodb.org/mongo-driver v1.17.7
require_min github.com/shamaton/msgpack/v2 v2.4.1
require_min golang.org/x/crypto v0.53.0
require_min golang.org/x/image v0.18.0
require_otel_not_vulnerable

printf 'dependency security graph ok\n'
