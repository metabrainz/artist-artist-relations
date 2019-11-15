#!/usr/bin/env python3

import sys
import json
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable
import bz2
import ujson

DUMP_QUERY = '''SELECT CAST(count AS float) / (select max(count) from artist_credit_artist_credit_relations) as score,
                       ac0.id, string_agg(concat(acn0.name, acn0.join_phrase), ''),
                       ac1.id, string_agg(concat(acn1.name, acn1.join_phrase), '')
                  FROM artist_credit_artist_credit_relations arr
                  JOIN artist_credit ac0 ON arr.artist_credit_0 = ac0.id
                  JOIN artist_credit_name acn0 ON arr.artist_credit_0 = acn0.artist_credit
                  JOIN artist_credit ac1 ON arr.artist_credit_1 = ac1.id
                  JOIN artist_credit_name acn1 ON arr.artist_credit_1 = acn1.artist_credit
                 WHERE count > 3
              GROUP BY ac0.id, acn0.position, ac1.id, acn1.position, arr.count
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
                        'id_0' : row[1],
                        'name_0' : row[2],
                        'id_1' : row[3],
                        'name_1' : row[4]
                    }) + "\n")


if __name__ == "__main__":
    dump_table("artist_credit_artist_credit_relations.tar.bz2")
