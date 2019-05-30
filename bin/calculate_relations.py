#!/usr/bin/env python3

import sys
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable

ARTIST_MBIDS_TO_EXCLUDE = [
    'f731ccc4-e22a-43af-a747-64213329e088', # anonymous
    '125ec42a-7229-4250-afc5-e057484327fe', # unknown
]

CREATE_RELATIONS_ARTISTS_QUERY = '''
    SELECT DISTINCT r.gid as release_mbid, a.gid as artist_mbid, a.name as artist_name
      FROM release r
      JOIN medium m ON m.release = r.id AND r.artist_credit = 1
      JOIN track t ON t.medium = m.id
      JOIN artist_credit ac ON t.artist_credit = ac.id
      JOIN artist_credit_name acm ON acm.artist_credit = ac.id
      JOIN artist a ON acn.artist = a.id
'''

CREATE_RELATIONS_ARTIST_CREDITS_QUERY = '''
    SELECT DISTINCT r.id as release_id, ac.id as artist_credit_id
      FROM release r
      JOIN medium m ON m.release = r.id AND r.artist_credit = 1
      JOIN track t ON t.medium = m.id
      JOIN artist_credit ac ON t.artist_credit = ac.id
'''

CREATE_RELATIONS_TABLE_QUERY = '''
    CREATE TABLE artist_artist_relations (
        count integer, 
        artist_credit_0 integer, 
        artist_credit_1 integer
    )
'''

TRUNCATE_RELATIONS_TABLE_QUERY = '''
    TRUNCATE artist_artist_relations
'''

def create_schema():
    pass


def create_or_truncate_table(conn):

    try:
        with conn.cursor() as curs:
            curs.execute(CREATE_RELATIONS_TABLE_QUERY)
    except DuplicateTable as err:
        conn.rollback() 
        try:
            with conn.cursor() as curs:
                curs.execute(TRUNCATE_RELATIONS_TABLE_QUERY)
        except OperationalError as err:
            print("failed to truncate existing table")


def insert_artist_pairs(artists, relations):
    for a0 in artists:
        for a1 in artists:
            if a0 == a1:
                continue

            if a0 < a1:
                k = "%d=%d" % (a0, a1)
            else:
                k = "%d=%d" % (a1, a0)

            try:
                relations[k][0] += 1
            except KeyError:
                relations[k] = [ 1, a0, a1 ]

def insert_rows(curs, values):

    query = "INSERT INTO artist_artist_relations VALUES " + join(",", values)
    try:
        curs.execute(query)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")

            
def dump_similarities(conn, relations):

    create_or_truncate_table(conn)

    values = []
    with conn.cursor() as curs:

        for k in relations:
            r = relations[k]
            values.append("(%d, %d, %d)" % (r[0], r[1], r[2]))

            if len(values > 1000):
                insert_rows(curs, values)
                values = []

        if len(values):
            insert_rows(curs, values)




def calculate_artist_similarities():

    relations = {}
    release_id = 0
    artists = []

    print("query for va tracks")
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

                artists.append(row[0])
                release_id = row[0]
                count += 1

        print("save relations to new table")
        dump_similarities(conn, relations)


if __name__ == "__main__":
    calculate_artist_similarities()
