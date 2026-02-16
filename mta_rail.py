#!/usr/bin/env python3

import os
import argparse
import requests
from dataclasses import dataclass
from typing import List, Optional
from google.transit import gtfs_realtime_pb2
import time
import json
from collections import OrderedDict

parser = argparse.ArgumentParser(description="NYC MTA Subway Train Tracker")
route_ids = ['1', '2', '3', '4', '5', '6', '6X', '7', '7X', 'GS', 'A', 'B', 'C', 'D', 'E', 'F', 'FX', 'FS', 'G', 'J', 'L', 'M', 'N', 'Q', 'R', 'H', 'W', 'Z', 'SI']
parser.add_argument('-l', '-r', '--route', type=str, choices=route_ids, default='6', help='Subway route to track')
parser.add_argument('-st', '--self-test', action='store_true', help='Run self-test to display route colors and names')
args = parser.parse_args()

line_to_long_name = {
    "1": "Broadway - 7 Avenue Local",
    "2": "7 Avenue Express",
    "3": "7 Avenue Express",
    "4": "Lexington Avenue Express",
    "5": "Lexington Avenue Express",
    "5X": "Lexington Avenue Express",
    "6": "Lexington Avenue Local",
    "6X": "Pelham Bay Park Express",
    "7": "Flushing Local",
    "7X": "Flushing Express",
    "GS": "42 St Shuttle",
    "A": "8 Avenue Express",
    "B": "6 Avenue Express",
    "C": "8 Avenue Local",
    "D": "6 Avenue Express",
    "E": "8 Avenue Local",
    "F": "Queens Blvd Express/ 6 Av Local",
    "FX": "Brooklyn F Express",
    "FS": "Franklin Avenue Shuttle",
    "G": "Brooklyn-Queens Crosstown",
    "J": "Nassau St Local",
    "L": "14 St-Canarsie Local",
    "M": "Queens Blvd Local/6 Av Local",
    "N": "Broadway Local",
    "Q": "Broadway Express",
    "R": "Broadway Local",
    "H": "Rockaway Park Shuttle",
    "W": "Broadway Local",
    "Z": "Nassau St Express",
    "SI": "Staten Island Railway"
}

colors = {
    "1": "\033[1m\033[38;2;255;255;255m\033[48;2;216;34;51m",
    "2": "\033[1m\033[38;2;255;255;255m\033[48;2;216;34;51m",
    "3": "\033[1m\033[38;2;255;255;255m\033[48;2;216;34;51m",
    "4": "\033[1m\033[38;2;255;255;255m\033[48;2;0;153;82m",
    "5": "\033[1m\033[38;2;255;255;255m\033[48;2;0;153;82m",
    "5X": "\033[1m\033[38;2;255;255;255m\033[48;2;0;153;82m",
    "6": "\033[1m\033[38;2;255;255;255m\033[48;2;0;153;82m",
    "6X": "\033[1m\033[38;2;255;255;255m\033[48;2;0;153;82m",
    "7": "\033[1m\033[38;2;255;255;255m\033[48;2;154;56;161m",
    "7X": "\033[1m\033[38;2;255;255;255m\033[48;2;154;56;161m",
    "GS": "\033[1m\033[38;2;255;255;255m\033[48;2;124;133;140m",
    "A": "\033[1m\033[38;2;255;255;255m\033[48;2;0;98;207m",
    "B": "\033[1m\033[38;2;255;255;255m\033[48;2;235;104;0m",
    "C": "\033[1m\033[38;2;255;255;255m\033[48;2;0;98;207m",
    "D": "\033[1m\033[38;2;255;255;255m\033[48;2;235;104;0m",
    "E": "\033[1m\033[38;2;255;255;255m\033[48;2;0;98;207m",
    "F": "\033[1m\033[38;2;255;255;255m\033[48;2;235;104;0m",
    "FX": "\033[1m\033[38;2;255;255;255m\033[48;2;235;104;0m",
    "FS": "\033[1m\033[38;2;255;255;255m\033[48;2;124;133;140m",
    "G": "\033[1m\033[38;2;255;255;255m\033[48;2;121;149;52m",
    "J": "\033[1m\033[38;2;255;255;255m\033[48;2;142;92;51m",
    "L": "\033[1m\033[38;2;255;255;255m\033[48;2;124;133;140m",
    "M": "\033[1m\033[38;2;255;255;255m\033[48;2;235;104;0m",
    "N": "\033[1m\033[38;2;0;0;0m\033[48;2;246;188;38m",
    "Q": "\033[1m\033[38;2;0;0;0m\033[48;2;246;188;38m",
    "R": "\033[1m\033[38;2;0;0;0m\033[48;2;246;188;38m",
    "H": "\033[1m\033[38;2;255;255;255m\033[48;2;124;133;140m",
    "W": "\033[1m\033[38;2;0;0;0m\033[48;2;246;188;38m",
    "Z": "\033[1m\033[38;2;255;255;255m\033[48;2;142;92;51m",
    "SI": "\033[1m\033[38;2;255;255;255m\033[48;2;8;23;156m",
}

realtime_feed_urls = {
    "A,C,E,S^R": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "B,D,F,M,S^F": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "J,Z": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "N,Q,R,W": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "1,2,3,4,5,6,7,S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "SIR": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si"
}

line_to_url = {
    "1": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "2": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "3": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "4": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "5": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "5X": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "6": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "6X": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "7": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "7X": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "GS": realtime_feed_urls["1,2,3,4,5,6,7,S"],
    "A": realtime_feed_urls["A,C,E,S^R"],
    "B": realtime_feed_urls["B,D,F,M,S^F"],
    "C": realtime_feed_urls["A,C,E,S^R"],
    "D": realtime_feed_urls["B,D,F,M,S^F"],
    "E": realtime_feed_urls["A,C,E,S^R"],
    "F": realtime_feed_urls["B,D,F,M,S^F"],
    "FX": realtime_feed_urls["B,D,F,M,S^F"],
    "FS": realtime_feed_urls["B,D,F,M,S^F"],
    "G": realtime_feed_urls["G"],
    "J": realtime_feed_urls["J,Z"],
    "L": realtime_feed_urls["L"],
    "M": realtime_feed_urls["B,D,F,M,S^F"],
    "N": realtime_feed_urls["N,Q,R,W"],
    "Q": realtime_feed_urls["N,Q,R,W"],
    "R": realtime_feed_urls["N,Q,R,W"],
    "H": realtime_feed_urls["A,C,E,S^R"],
    "W": realtime_feed_urls["N,Q,R,W"],
    "Z": realtime_feed_urls["J,Z"],
    "SI": realtime_feed_urls["SIR"]
}

@dataclass
class Train:
    trip_id: str
    route_id: str
    start_date: str
    next_stop_id: str
    time_until: float
    direction: Optional[str] = None
    next_station_index: Optional[int] = -1
    direction_char: Optional[str] = None
    vehicle_stop_id: Optional[str] = None
    current_status: Optional[str] = None
    section_name: Optional[str] = None

    def __str__(self):
        eta = f"{int(self.time_until)}s" if self.time_until is not None else "n/a"
        # vehicle_info = f" | veh_stop: {self.vehicle_stop_id}" if self.vehicle_stop_id else ""
        status_info = f"{self.current_status}" if self.current_status else ""
        
        # Use the TrainGetter instance to get the station name from the base stop ID
        base_stop_id = self.next_stop_id[:-1] if self.direction_char else self.next_stop_id
        next_station_name = traingetter.station_id_to_name(base_stop_id) or self.next_stop_id
        
        direction_prefix = self.direction_char if self.direction_char else ""

        return f"""
{colors[args.route] + f" {direction_prefix} " + "\033[0m"} {"\033[1;44m" + self.trip_id + "\033[0m"} {"\033[1;33m" + status_info}\n{next_station_name} in {eta}\033[0m"""

def _secs_until(arrival_ts: int) -> float:
    try:
        now = time.time()
        remaining = float(arrival_ts) - now
        return remaining if remaining > 0 else 0.0
    except Exception:
        return 0.0

class TrainGetter():
    def __init__(self) -> None:

        # Load station data from mta_subway_stations.json (as OrderedDict)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(script_dir, "mta_subway_stations.json")
        with open(json_path, "r") as f:
            station_data = json.load(f, object_pairs_hook=OrderedDict)
        
        # Load mappings for the specified route
        self.route_data = station_data.get(args.route, OrderedDict())
        if args.route in ['GS', 'FS', 'H']:
            self.route_data = station_data.get('S', OrderedDict())
            match args.route:
                case 'GS':
                    self.route_data = {'42 St Shuttle (Manhattan)': self.route_data.get('42 St Shuttle (Manhattan)', {})}
                case 'FS':
                    self.route_data = {'Franklin Shuttle (Brooklyn)': self.route_data.get('Franklin Shuttle (Brooklyn)', {})}
                case 'H':
                    self.route_data = {'Rockaway Shuttle (Queens)': self.route_data.get('Rockaway Shuttle (Queens)', {})}

        # --- Precompute helpers from the new nested structure ---
        self.stop_id_to_name = OrderedDict()
        self.name_to_index = OrderedDict()
        self.stop_id_to_section = {}
        self.section_endpoints = {}

        flat_station_list = []
        for section_name, stations in self.route_data.items():
            if stations:
                # The endpoint of a section is its first station
                first_stop_id = next(iter(stations))
                self.section_endpoints[section_name] = stations[first_stop_id]

            for stop_id, stop_name in stations.items():
                self.stop_id_to_name[stop_id] = stop_name
                self.stop_id_to_section[stop_id] = section_name
                if stop_name not in flat_station_list:
                    flat_station_list.append(stop_name)

        self.name_to_index = {name: i for i, name in enumerate(flat_station_list)}
        self.station_names = flat_station_list
        # --- End precomputation ---

        self._trip_direction_map = None

    def station_id_to_name(self, stop_id: str) -> Optional[str]:
        return self.stop_id_to_name.get(stop_id)

    def station_name_to_index(self, name: str) -> Optional[int]:
        return self.name_to_index.get(name)

    def get_trains(self, feed: gtfs_realtime_pb2.FeedMessage) -> List[Train]:
        trip_updates = {}
        vehicles = {}
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                try:
                    trip = entity.trip_update.trip
                    trip_id = getattr(trip, "trip_id", None)
                    route_id = getattr(trip, "route_id", None)
                    if trip_id and route_id == args.route:
                        trip_updates[trip_id] = entity.trip_update
                except Exception:
                    continue
            if entity.HasField('vehicle'):
                try:
                    veh_trip = entity.vehicle.trip
                    veh_trip_id = getattr(veh_trip, "trip_id", None)
                    if veh_trip_id:
                        vehicles[veh_trip_id] = entity.vehicle
                except Exception:
                    continue

        trains: List[Train] = []
        for trip_id, tu in trip_updates.items():
            try:
                route_id = tu.trip.route_id
            except Exception:
                continue
            if route_id != args.route:
                continue

            next_stop_id = "(no stop)"
            arrival_ts = None
            for stu in tu.stop_time_update:
                at = getattr(stu, "arrival", None)
                dt = getattr(stu, "departure", None)
                if at and getattr(at, "time", 0):
                    arrival_ts = getattr(at, "time")
                    next_stop_id = getattr(stu, "stop_id", "(no stop)")
                    break
                if dt and getattr(dt, "time", 0):
                    arrival_ts = getattr(dt, "time")
                    next_stop_id = getattr(stu, "stop_id", "(no stop)")
                    break

            secs = _secs_until(arrival_ts) if arrival_ts else 0.0

            direction_char = None
            if len(next_stop_id) > 1 and next_stop_id[-1] in ('N', 'S'):
                direction_char = next_stop_id[-1]

            direction = None
            try:
                direction = tu.trip.direction_id
            except Exception:
                pass

            veh = vehicles.get(trip_id)
            veh_stop = None
            status = None
            if veh:
                try:
                    veh_stop = veh.stop_id if hasattr(veh, "stop_id") else None
                except Exception:
                    veh_stop = None
                try:
                    status = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(veh.current_status)
                except Exception:
                    status = str(getattr(veh, "current_status", ""))

            base_stop_id = next_stop_id[:-1] if direction_char else next_stop_id
            station_name = self.station_id_to_name(base_stop_id)
            station_index = self.station_name_to_index(station_name) if station_name else -1
            section_name = self.stop_id_to_section.get(base_stop_id)

            trains.append(Train(
                trip_id=trip_id,
                route_id=route_id,
                start_date=tu.trip.start_date if hasattr(tu.trip, "start_date") else "",
                next_stop_id=next_stop_id,
                time_until=secs,
                direction=direction,
                next_station_index=station_index,
                direction_char=direction_char,
                vehicle_stop_id=veh_stop,
                current_status=status,
                section_name=section_name
            ))

        def get_sort_key(train: Train):
            # Direction 0 is typically 'North' or towards the higher-indexed station
            is_to_endpoint = (train.direction_char == 'N')
            
            # Primary sort: station index.
            primary_sort = train.next_station_index - is_to_endpoint
            primary_sort *= -1

            # Secondary sort: time_until within the same station index block
            secondary_sort = train.time_until

            # Invert secondary sort based on direction to maintain order within a block
            secondary_sort *= (1 if is_to_endpoint else -1)

            return (primary_sort, secondary_sort)

        try:
            trains = sorted(trains, key=get_sort_key)
        except Exception:
            pass
        
        return trains

def self_test():
    print("NYC MTA Subway Routes:\n")
    for route in route_ids:
        color = colors.get(route, "\033[0m")
        print(f"{color}{route}: {line_to_long_name[route]}\033[0m")
    print("\n")
    print(len(route_ids), "routes total.")
    print(len(line_to_long_name), "long names total.")
    print(len(colors), "ANSI colors total.")
    print(len(line_to_url), "line to URL mappings total.")

if __name__ == "__main__":
    if args.self_test:
        self_test()
        exit()

    if args.route:
        traingetter = TrainGetter() # Instantiate the class
        feed = gtfs_realtime_pb2.FeedMessage()
        url = line_to_url[args.route]
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            feed.ParseFromString(response.content)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            exit()

        trains = traingetter.get_trains(feed)

        # --- Print the results by section ---
        color = colors.get(args.route, "\033[0m")
        print(color + f" {args.route}: {line_to_long_name.get(args.route, '')} " + "\033[0m")
        
        if not traingetter.route_data:
            print(f"No station data found for route {args.route} in mta_subway_stations.json")
            exit()

        if not trains:
            print("No trip updates for this route in feed.")
        
        # Group trains by section
        trains_by_section = {section: [] for section in traingetter.route_data.keys()}
        for t in trains:
            if t.section_name and t.section_name in trains_by_section:
                trains_by_section[t.section_name].append(t)

        # Print each section and its trains
        for section_name, section_trains in trains_by_section.items():
            print(f"\n\033[1;4m{section_name}\033[0m")
            if section_trains:
                for t in section_trains:
                    print(t)
            else:
                # It's okay if no trains are on a section
                pass