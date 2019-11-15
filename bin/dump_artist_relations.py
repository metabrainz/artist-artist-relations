#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable
import bz2
import ujson

DUMP_QUERY = '''SELECT CAST(count AS float) / (select max(count) from artist_artist_relations) as score,
                       a0.gid, a0.name, a1.gid, a1.name
                  FROM artist_artist_relations arr
                  JOIN artist a0 ON arr.artist_0 = a0.id
                  JOIN artist a1 ON arr.artist_1 = a1.id
                 WHERE count > 3
              ORDER BY score DESC
             '''

def dump_table(filename):
    with bz2.open(filename, "wt") as f:
        with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
            with conn.cursor() as curs:
                curs.execute(DUMP_QUERY)
                while True:
                    row = curs.fetchone()
                    if not row:
                        return None

                    f.write(ujson.dumps({
                        'score' : row[0],
                        'mbid_0' : row[1],
                        'name_0' : row[2],
                        'mbid_1' : row[3],
                        'name_1' : row[4]
                    }) + "\n")


if __name__ == "__main__":
    dump_table("artist_artist_relations.json.bz2")
