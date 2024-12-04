#!/usr/bin/env python3

from google.transit import gtfs_realtime_pb2
from pathlib import Path
from datetime import datetime
import requests
import argparse
import csv
import time

"""
stop_times.txt shows scheduled dep/arr times for all train numbers
"""

college_park_nb_id = 12018
college_park_sb_id = 12015
new_carrollton_nb_id = 11989
new_carrollton_sb_id = 11988

schedule_relationship = ['scheduled', 'added', 'unscheduled', 'canceled', 'null', 'replacement', 'duplicated', 'deleted']

def parse_marc(folder_path):
    marc_info = {}
    for file in Path(folder_path).iterdir():
        if file.stem in {'stops', 'trips', 'routes'}:
            marc_info[file.stem] = {}
            with open(file) as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    marc_info[file.stem][list(row.values())[0]] = row
    return marc_info

def main(args):
    marc_info = parse_marc(args.marc_gtfs) 

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get('https://mdotmta-gtfs-rt.s3.amazonaws.com/MARC+RT/marc-tu.pb')
    feed.ParseFromString(response.content)
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip_update = entity.trip_update
            trip_desc = trip_update.trip
            trip_dict = marc_info['trips'][trip_desc.trip_id]
            route_dict = marc_info['routes'][trip_desc.route_id]
            sched_relation = schedule_relationship[trip_desc.schedule_relationship]
            dest = list(trip_update.stop_time_update)[-1]
            print(f"Trip update for {sched_relation} {route_dict['route_long_name'].split()[0]} line {trip_dict['trip_short_name']} to {marc_info['stops'][dest.stop_id]['stop_name']}:")
            curr_time = time.time()
            for stu in trip_update.stop_time_update:
                if stu.HasField('arrival'):
                    ts = stu.arrival.time
                    if ts > curr_time:
                        arr_time = datetime.fromtimestamp(ts)
                        print(f"Arriving {arr_time} at {marc_info['stops'][stu.stop_id]['stop_name']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--marc_gtfs', type=str, required=True, help="Path MARC static GFTS")
    args = parser.parse_args()
    main(args)
    