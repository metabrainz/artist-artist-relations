#!/bin/bash

if [ "$1" = "build" ]
then
    docker build -f Dockerfile -t metabrainz/artist-artist-relations .
    exit
fi

if [ "$1" = "up" ]
then
    docker run -d --rm --name artist-artist-relations-host -v `pwd`:/code/relations --network=musicbrainz-docker_default \
        metabrainz/artist-artist-relations python3 bin/_dummy_loop.py
    exit
fi

if [ "$1" = "down" ]
then
    docker rm -f artist-artist-relations-host
    exit
fi

docker exec -it artist-artist-relations-host python3 $@
