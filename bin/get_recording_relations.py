#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

SELECT_RECORDING_QUERY = '''  
    SELECT id 
      FROM recording
     WHERE gid = %s 
'''

SELECT_RELATIONS_QUERY = '''
    SELECT count, rrr.recording_0, r0.name AS recording_name_0, ac0.name AS artist_credit_0,
           rrr.recording_1, r1.name AS recording_name_1, ac1.name AS artist_credit_1
      FROM recording_recording_relations rrr
      JOIN recording r0 ON rrr.recording_0 = r0.id
      JOIN artist_credit ac0 ON r0.artist_credit = ac0.id
      JOIN recording r1 ON rrr.recording_1 = r1.id
      JOIN artist_credit ac1 ON r1.artist_credit = ac1.id
     WHERE (rrr.recording_0 = %s OR rrr.recording_1 = %s)
       AND count > 2
  ORDER BY count desc
'''

def get_recording_similarities(recording_mbid):

    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            curs.execute(SELECT_RECORDING_QUERY, (recording_mbid,))
            row = curs.fetchone()
            if not row:
                return None

            recording_id = row[0]
            curs.execute(SELECT_RELATIONS_QUERY, (recording_id, recording_id))
            relations = []
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if recording_id == row[1]: 
                    relations.append({
                        'count' : row[0],
                        'recording_id' : row[4],
                        'recording_name' : row[5],
                        'artist_name' : row[6]
                    })
                else:
                    relations.append({
                        'count' : row[0],
                        'recording_id' : row[1],
                        'recording_name' : row[2],
                        'artist_name' : row[3]
                    })

            return { 
                'recording_id' : recording_id,
                'recording_mbid' : recording_mbid,
                'relations' : relations
            }


if __name__ == "__main__":
    relations = get_recording_similarities(sys.argv[1])
    if relations:
        print("Related recordings for '%s'" % relations['recording_mbid'])
        for relation in relations['relations']:
            print("%5d %-30s %s" % (relation['count'], relation['artist_name'][:29], relation['recording_name']))
    else:
        print("Found no relations for recording '%s'" % sys.argv[1])
