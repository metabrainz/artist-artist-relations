#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

SELECT_ARTIST_QUERY = '''  
    SELECT id 
      FROM artist_credit 
     WHERE name = 'Testament' 
  ORDER BY ref_count DESC 
     LIMIT 1
'''

SELECT_RELATIONS_QUERY = '''
    SELECT count, arr.artist_credit_0, ac0.name AS artist_credit_name_0, arr.artist_credit_1, ac1.name AS artist_credit_name_1 
      FROM artist_artist_relations arr
      JOIN artist_credit ac0 ON arr.artist_credit_0 = ac0.id
      JOIN artist_credit ac1 ON arr.artist_credit_1 = ac1.id
     WHERE arr.artist_credit_0 = %d
        OR arr.artist_credit_1 = %d
'''

def get_artist_similarities():

    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            count = 0
            curs.execute(CREATE_RELATIONS_ARTIST_CREDITS_QUERY)
            print("load va tracks")
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if release_id != row[0] and artists:
                    insert_artist_pairs(artists, relations)
                    artists = []

                artists.append(row[1])
                release_id = row[0]
                count += 1

        print("save relations to new table")
        dump_similarities(conn, relations)


if __name__ == "__main__":
    calculate_artist_similarities()
