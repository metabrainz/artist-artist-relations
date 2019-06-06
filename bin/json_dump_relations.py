#!/usr/bin/env python3

import sys
import json
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

DUMP_QUERY = '''SELECT count, artist_credit_0, artist_credit_1 FROM artist_artist_relations'''

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
                    'artist_credit_0' : row[1],
                    'artist_credit_1' : row[2]
                }))


if __name__ == "__main__":
    dump_table()
