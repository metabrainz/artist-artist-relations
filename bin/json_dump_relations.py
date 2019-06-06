#!/usr/bin/env python3

import sys
import json
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

DUMP_QUERY = '''SELECT count, a0.gid, a1.gid
                  FROM artist_artist_relations arr
                  JOIN artist a0 ON arr.artist_0 = a0.id
                  JOIN artist a1 ON arr.artist_1 = a1.id
             '''

def dump_table():
    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(DUMP_QUERY)
            while True:
                row = curs.fetchone()
                if not row:
                    return None

                print(json.dumps({
                    'count' : row[0],
                    'artist_mbid_0' : row[1],
                    'artist_mbid_1' : row[2]
                }))


if __name__ == "__main__":
    dump_table()
