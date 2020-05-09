#!/usr/bin/env python3

import sys
import pprint
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject

ARTIST_MBIDS_TO_EXCLUDE = [
    'f731ccc4-e22a-43af-a747-64213329e088', # anonymous
    '125ec42a-7229-4250-afc5-e057484327fe', # unknown
]

CREATE_RELATIONS_RECORDINGS_QUERY = '''
    SELECT DISTINCT rel.id as release_id, r.id as recording_id, r.gid as recording_mbid
      FROM release rel
      JOIN medium m ON m.release = rel.id AND rel.artist_credit = 1
      JOIN track t ON t.medium = m.id
      JOIN recording r ON t.recording = r.id
'''

CREATE_RELATIONS_TABLE_QUERY = '''
    CREATE TABLE recording_recording_relations (
        count integer, 
        recording_0 integer, 
        recording_1 integer
    )
'''

TRUNCATE_RELATIONS_TABLE_QUERY = '''
    TRUNCATE recording_recording_relations
'''

CREATE_INDEX_QUERIES = [ '''
    CREATE INDEX recording_recording_relations_recording_0_ndx 
              ON recording_recording_relations (recording_0)
''',
'''
    CREATE INDEX recording_recording_relations_recording_1_ndx 
              ON recording_recording_relations (recording_1)
''']

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
                conn.commit()

            with conn.cursor() as curs:
                try:
                    curs.execute("DROP INDEX recording_recording_relations_recording_0_ndx")
                    conn.commit()
                except UndefinedObject as err:
                    conn.rollback()

                try:
                    curs.execute("DROP INDEX recording_recording_relations_recording_1_ndx")
                    conn.commit()
                except UndefinedObject as err:
                    conn.rollback()

        except OperationalError as err:
            print("failed to truncate existing table")
            conn.rollback()


def create_indexes(conn):
    try:
        with conn.cursor() as curs:
            for query in CREATE_INDEX_QUERIES:
                curs.execute(query)
            conn.commit()
    except OperationalError as err:
        conn.rollback()
        print("creating indexes failed.")


def insert_recording_pairs(recordings, relations):

    for r0 in recordings:
        for r1 in recordings:
            if r0 == r1:
                continue

            if r0 < r1:
                k = "%d=%d" % (r0, r1)
            else:
                k = "%d=%d" % (r1, r0)

            try:
                relations[k][0] += 1
            except KeyError:
                relations[k] = [ 1, r0, r1 ]


def insert_rows(curs, values):

    query = "INSERT INTO recording_recording_relations VALUES " + ",".join(values)
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
            if r[0] > 2:
                values.append("(%d, %d, %d)" % (r[0], r[1], r[2]))

            if len(values) > 1000:
                insert_rows(curs, values)
                conn.commit()
                values = []

        if len(values):
            insert_rows(curs, values)
            conn.commit()



def calculate_recording_similarities():

    relations = {}
    release_id = 0
    recordings = []

    print("query for va tracks")
    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            count = 0
            curs.execute(CREATE_RELATIONS_RECORDINGS_QUERY)
            print("load va tracks")
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if row[2] in ARTIST_MBIDS_TO_EXCLUDE:
                    continue

                if release_id != row[0] and recordings:
                    insert_recording_pairs(recordings, relations)
                    recordings = []

                recordings.append(row[1])
                release_id = row[0]
                count += 1

        print("save relations to new table")
        dump_similarities(conn, relations)
        print("create indexes")
        create_indexes(conn)


if __name__ == "__main__":
    calculate_recording_similarities()
