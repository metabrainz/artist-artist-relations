#!/bin/bash

if [ "$1" = "build" ]
then
    docker build -f Dockerfile -t metabrainz/musicbrainz-relations .
    exit
fi

if [ "$1" = "up" ]
then
    docker run -d --rm --name musicbrainz-relations-host -v `pwd`:/code/relations --network=musicbrainz-docker_default \
        metabrainz/musicbrainz-relations python3 bin/_dummy_loop.py
    exit
fi

if [ "$1" = "down" ]
then
    docker rm -f musicbrainz-relations-host
    exit
fi

docker exec -it musicbrainz-relations-host python3 $@
