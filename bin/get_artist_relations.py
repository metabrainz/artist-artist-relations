#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

SELECT_ARTIST_QUERY = '''  
    SELECT id 
      FROM artist
     WHERE name = %s 
     LIMIT 1
'''

SELECT_RELATIONS_QUERY = '''
    SELECT count, arr.artist_0, a0.name AS artist_name_0, arr.artist_1, a1.name AS artist_name_1 
      FROM artist_artist_relations arr
      JOIN artist a0 ON arr.artist_0 = a0.id
      JOIN artist a1 ON arr.artist_1 = a1.id
     WHERE (arr.artist_0 = %s OR arr.artist_1 = %s)
       AND count > 2
  ORDER BY count desc
'''

def get_artist_similarities(artist_name):

    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_ARTIST_QUERY, (artist_name,))
            row = curs.fetchone()
            if not row:
                return None

            artist_id = row[0]
            curs.execute(SELECT_RELATIONS_QUERY, (artist_id, artist_id))
            relations = []
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if artist_id == row[1]: 
                    relations.append({
                        'count' : row[0],
                        'artist_id' : row[3],
                        'artist_name' : row[4]
                    })
                else:
                    relations.append({
                        'count' : row[0],
                        'artist_id' : row[1],
                        'artist_name' : row[2]
                    })

            return { 
                'artist' : artist_id,
                'artist_name' : artist_name,
                'relations' : relations
            }


if __name__ == "__main__":
    relations = get_artist_similarities(sys.argv[1])
    if relations:
        print("Related artists for '%s'" % relations['artist_name'])
        for relation in relations['relations']:
            print("%5d %s" % (relation['count'], relation['artist_name']))
    else:
        print("Found no relations for artist '%s'" % sys.argv[1])
