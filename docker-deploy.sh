#!/bin/bash

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin && \
docker build . -t migrate/migrate:"$TRAVIS_TAG" && \
docker push migrate/migrate:"$TRAVIS_TAG"
