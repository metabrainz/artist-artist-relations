#!/usr/bin/env python3

import sys
import csv

'''
   create table artist_artist_relations (count integer , artist0_name text, artist0_mbid text, artist1_name text, artist1_mbid text);

   \copy artist_artist_relations from '/tmp/artist-artist.csv' delimiter ',' escape '"' csv
'''

temp_query = '''
   SELECT r.id as release_id, a.gid as artist_mbid, a.name as artist_name 
     INTO artist_relations
     FROM artist_credit as ac, artist_credit_name as acm, artist as a, release as r, medium as m, track as t 
    WHERE t.artist_credit = ac.id and ac.id = acm.artist_credit and acm.artist = a.id and r.artist_credit = 1 and m.release = r.id and m.id = t.medium
'''

fetch_query = '''
   \copy (SELECT * FROM artist_relations order by release_id) to '/tmp/va_tracks.csv' with csv
'''

artist_mbids_to_exclude = [
    'f731ccc4-e22a-43af-a747-64213329e088', # anonymous
    '125ec42a-7229-4250-afc5-e057484327fe', # unknown
]

def insert_artist_pairs(artists, relations):
    for a0, a0_name in artists:
        for a1, a1_name in artists:
            if a0[0] == a1[0]:
                continue

            if a0 in artist_mbids_to_exclude or a1 in artist_mbids_to_exclude:
                continue

            if a0 < a1:
                k = "%s=%s" % (a0, a1)
            else:
                k = "%s=%s" % (a1, a0)

            try:
                relations[k]["count"] += 1
            except KeyError:
                relations[k] = {
                    "count" : 1,
                    "artist0_mbid" : a0,
                    "artist0_name" : a0_name,
                    "artist1_mbid" : a1,
                    "artist1_name" : a1_name,
                }

            
def dump_similarities(relations):

    with open('artist-artist.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        for k in relations:
            r = relations[k]
            writer.writerow([ r['count'],
                              r['artist0_name'],
                              r['artist0_mbid'],
                              r['artist1_name'],
                              r['artist1_mbid']
                            ])


def calculate_artist_similarities(in_file):

    relations = {}
    release_id = 0
    artists = []
    with open(in_file) as csvfile:
        read_csv = csv.reader(csvfile, delimiter=',')
        count = 0
        for row in read_csv:
            if release_id != row[0] and artists:
                insert_artist_pairs(artists, relations)
                artists = []

            artists.append((row[1], row[2]))
            release_id = row[0]
            count += 1

    dump_similarities(relations)


if __name__ == "__main__":
    calculate_artist_similarities(sys.argv[1])

