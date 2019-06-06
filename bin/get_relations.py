#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

SELECT_ARTIST_QUERY = '''  
    SELECT id 
      FROM artist_credit 
     WHERE name = %s 
  ORDER BY ref_count DESC 
     LIMIT 1
'''

SELECT_RELATIONS_QUERY = '''
    SELECT count, arr.artist_credit_0, ac0.name AS artist_credit_name_0, arr.artist_credit_1, ac1.name AS artist_credit_name_1 
      FROM artist_artist_relations arr
      JOIN artist_credit ac0 ON arr.artist_credit_0 = ac0.id
      JOIN artist_credit ac1 ON arr.artist_credit_1 = ac1.id
     WHERE (arr.artist_credit_0 = %s OR arr.artist_credit_1 = %s)
       AND count > 2
  ORDER BY count desc
'''

def get_artist_similarities(artist_credit_name):

    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_ARTIST_QUERY, (artist_credit_name,))
            row = curs.fetchone()
            if not row:
                return None

            artist_credit_id = row[0]
            curs.execute(SELECT_RELATIONS_QUERY, (artist_credit_id, artist_credit_id))
            relations = []
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if artist_credit_id == row[1]: 
                    relations.append({
                        'count' : row[0],
                        'artist_credit_id' : row[3],
                        'artist_credit_name' : row[4]
                    })
                else:
                    relations.append({
                        'count' : row[0],
                        'artist_credit_id' : row[1],
                        'artist_credit_name' : row[2]
                    })

            return { 
                'artist_credit' : artist_credit_id,
                'artist_credit_name' : artist_credit_name,
                'relations' : relations
            }


if __name__ == "__main__":
    relations = get_artist_similarities(sys.argv[1])
    if relations:
        print("Related artists for '%s'" % relations['artist_credit_name'])
        for relation in relations['relations']:
            print("%5d %s" % (relation['count'], relation['artist_credit_name']))
    else:
        print("Found no relations for artist '%s'" % sys.argv[1])
