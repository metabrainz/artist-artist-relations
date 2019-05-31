To build the container:

docker build -f Dockerfile -t metabrainz/artist-relations .

To run the script:

docker run -it --network=musicbrainz-docker_default metabrainz/artist-relations
