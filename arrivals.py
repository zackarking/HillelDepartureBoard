#!/usr/bin/env python3

from google.transit import gtfs_realtime_pb2
from pathlib import Path
from datetime import datetime
import requests
import argparse
import csv
import time

"""
static GTFS:
routes.txt - maps route_id to the 3 route names
stops.txt - maps stop_id to station name
trips.txt - maps trip_id to route_id, service_id, and trip name
stop_times.txt - maps trip_id to arrival and departure times per stop_id
calendar.txt - maps service_id to active days of week and start-end dates
calendar_dates.txt - maps service_id to dates where it is added or removed

Parsing steps to get scheduled trip:
1. Parse stop_times into a map from desired stop_id to arrival time and trip_id
2. Lookup in trips.txt to add service_id to map value

Upon screen update:
1. Make copy of master schedule
2. Filter to arrival times in the next 99 mins
3. Filter to service_ids that are active on that day or are added for that day
4. Remove entries that realtime has cancelled

"""

college_park_nb_id = 12018
college_park_sb_id = 12015
new_carrollton_nb_id = 11989
new_carrollton_sb_id = 11988

schedule_relationship = ['scheduled', 'added', 'unscheduled', 'canceled', 'null', 'replacement', 'duplicated', 'deleted']

def parse_marc_schedule(folder_path):
    north_stop_id = college_park_nb_id
    south_stop_id = college_park_sb_id
    master = {north_stop_id: {}, south_stop_id: {}}

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

def get_marc(path):
    marc_info = parse_marc(path) 
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
            print(f"Trip update for {sched_relation} {route_dict['route_long_name'].split()[0]} line {trip_dict['trip_short_name']} to {trip_dict['trip_headsign']}:")
            curr_time = time.time()
            for stu in trip_update.stop_time_update:
                if stu.HasField('arrival'):
                    ts = stu.arrival.time
                    if ts > curr_time:
                        arr_time = datetime.fromtimestamp(ts)
                        print(f"Arriving {arr_time} at {marc_info['stops'][stu.stop_id]['stop_name']}")

def get_metro(code):
    key = 'e13626d03d8e4c03ac07f95541b3091b'
    url = f'http://api.wmata.com/StationPrediction.svc/json/GetPrediction/{code}?api_key={key}'
    resp = requests.get(url)
    data = resp.json()
    filtered = []
    for entry in data["Trains"]:
        if entry['DestinationName'] != 'No Passenger' and entry['Min'] not in ['ARR', 'BRD', 'DLY']:
            filtered.append(entry)
    by_dest = {}
    for entry in filtered:
        if entry['DestinationName'] in by_dest:
            by_dest[entry['DestinationName']].append(entry['Min'])           
        else:
            by_dest[entry['DestinationName']] = [entry['Min']]
    print(by_dest)

def main(args):
    if args.marc_gtfs is not None:
        get_marc(args.marc_gtfs)
    if args.metro_code is not None:
        get_metro(args.metro_code)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--marc_gtfs', type=str, default=None, help="Path to MARC static GFTS")
    parser.add_argument('--metro_code', type=str, default=None, help="Metro station code (CP is E09)")
    args = parser.parse_args()
    main(args)
    