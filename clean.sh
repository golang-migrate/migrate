#!/bin/bash
# Script to clean up old CLI artifacts

# Remove old CLI binary if it exists
if [ -f "./cli/main.go" ]; then
  echo "Removing old CLI binary at ./cli/main.go"
  rm -f ./cli/main.go
fi

if [ -f "./cli/version.go" ]; then
  echo "Removing old CLI version file at ./cli/version.go"
  rm -f ./cli/version.go
fi

# Remove old CLI build artifacts
if [ -d "./cli/build" ]; then
  echo "Removing old CLI build directory at ./cli/build"
  rm -rf ./cli/build
fi

echo "Cleanup complete"
