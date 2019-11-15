#!/usr/bin/env python3

import sys
import pprint
import psycopg2
from psycopg2.errors import OperationalError, DuplicateTable, UndefinedObject

ARTIST_CREDIT_IDS_TO_EXCLUDE = [
    15071, # anonymous
    97546, # unknown
]

CREATE_RELATIONS_ARTISTS_QUERY = '''
    SELECT DISTINCT r.id as release_id, ac.id as artist_id
      FROM release r
      JOIN medium m ON m.release = r.id AND r.artist_credit = 1
      JOIN track t ON t.medium = m.id
      JOIN artist_credit ac ON t.artist_credit = ac.id
      JOIN artist_credit_name acm ON acm.artist_credit = ac.id
      JOIN artist a ON acm.artist = a.id
'''

CREATE_RELATIONS_TABLE_QUERY = '''
    CREATE TABLE artist_credit_artist_credit_relations (
        count integer, 
        artist_credit_0 integer, 
        artist_credit_1 integer
    )
'''

TRUNCATE_RELATIONS_TABLE_QUERY = '''
    TRUNCATE artist_credit_artist_credit_relations
'''

CREATE_INDEX_QUERIES = [ '''
    CREATE INDEX artist_credit_artist_credit_relations_artist_0_ndx 
              ON artist_credit_artist_credit_relations (artist_credit_0)
''',
'''
    CREATE INDEX artist_credit_artist_credit_relations_artist_1_ndx 
              ON artist_credit_artist_credit_relations (artist_credit_1)
''']

def create_schema():
    pass


def create_or_truncate_table(conn):

    try:
        with conn.cursor() as curs:
            print("create table")
            curs.execute(CREATE_RELATIONS_TABLE_QUERY)

    except DuplicateTable as err:
        conn.rollback() 
        try:
            with conn.cursor() as curs:
                print("truncate")
                curs.execute(TRUNCATE_RELATIONS_TABLE_QUERY)
                conn.commit()

            with conn.cursor() as curs:
                print("drop indexes")
                try:
                    curs.execute("DROP INDEX artist_credit_artist_credit_relations_artist_0_ndx")
                    conn.commit()
                except UndefinedObject as err:
                    conn.rollback()

                try:
                    curs.execute("DROP INDEX artist_credit_artist_credit_relations_artist_1_ndx")
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


def insert_artist_credit_pairs(artist_credits, relations):

    for a0 in artist_credits:
        for a1 in artist_credits:
            if a0 == 1 or a1 == 1:
                continue 

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

    query = "INSERT INTO artist_credit_artist_credit_relations VALUES " + ",".join(values)
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



def calculate_artist_credit_similarities():

    relations = {}
    release_id = 0
    artist_credits = []

    print("query for va tracks")
    with psycopg2.connect('dbname=musicbrainz_db user=musicbrainz host=musicbrainz-docker_db_1 password=musicbrainz') as conn:
        with conn.cursor() as curs:
            count = 0
            curs.execute(CREATE_RELATIONS_ARTISTS_QUERY)
            print("load va tracks")
            while True:
                row = curs.fetchone()
                if not row:
                    break

                if row[1] in ARTIST_CREDIT_IDS_TO_EXCLUDE:
                    continue

                if release_id != row[0] and artist_credits:
                    insert_artist_credit_pairs(artist_credits, relations)
                    artist_credits = []

                artist_credits.append(row[1])
                release_id = row[0]
                count += 1

        print("save relations to new table")
        dump_similarities(conn, relations)
        print("create indexes")
        create_indexes(conn)


if __name__ == "__main__":
    calculate_artist_credit_similarities()
