import datetime
import math
from DbConnector import DbConnector
from haversine import haversine, Unit

class Task7: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def total_distance_km_2008_user_112(self):
        start = datetime.datetime(2008, 1, 1)
        end = datetime.datetime(2008, 12, 31)

        activity_collection = self.db["Activity"]
        test = activity_collection.find({
            'transportation_mode': 'walk',
            'user_id': '112',
            'start_date_time': {
                '$gte': start,
                '$lte': end
            }
        })

        trackpoint_collection = self.db["TrackPoint"]

        trackpoints = []

        for activities in test:
            # activity_id.append(activities['_id'])
            activity_tp = trackpoint_collection.find(
                {'activities_id': activities['_id']},
                {'lat': 1, 'lon': 1, 'activities_id': 1}).batch_size(1000)
            trackpoints.extend(list(activity_tp))
        
        prev_lat = None
        prev_lon = None
        total_distance = 0.0

        for track in trackpoints:
            lat = float(track['lat'])
            lon = float(track['lon'])

            if prev_lon is not None and prev_lat is not None:
                distance = haversine([prev_lat, prev_lon], [lat, lon], Unit.KILOMETERS)
                total_distance += distance

            prev_lat = lat
            prev_lon = lon

        
        print("total distance: "+total_distance)
     
            
            
        

def task7_main():
    program = Task7()
    program.total_distance_km_2008_user_112()
    program.connection.close_connection()

