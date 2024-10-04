import math
from tabulate import tabulate
from DbConnector import DbConnector

class Task7:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def haversine(self, lat1, lon1, lat2, lon2):
        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        r = 6371  # Radius of Earth in kilometers
        return r * c
    
    def retrieve_total_distance_2008_id_112(self):
        query = """SELECT TrackPoint.lat as lat1, TrackPoint.lon as lon1,
                          LEAD(TrackPoint.lat) OVER (PARTITION BY TrackPoint.activity_id ORDER BY TrackPoint.date_time) as lat2, 
                          LEAD(TrackPoint.lon) OVER (PARTITION BY TrackPoint.activity_id ORDER BY TrackPoint.date_time) as lon2
                    FROM db1.TrackPoint 
                    INNER JOIN Activity ON 
                        TrackPoint.activity_id = Activity.id
                    WHERE Activity.transportation_mode LIKE 'walk' 
                        AND Activity.user_id = 112
                        AND YEAR(start_date_time) LIKE 2008
                        AND YEAR(end_time) LIKE 2008;"""

        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()

        total_distance = 0  # Initialize total distance

        # Iterate through the trackpoints and calculate the distance
        for trackpoint in trackpoints:
            lat1, lon1, lat2, lon2 = trackpoint

            if lat2 is not None and lon2 is not None:
                # Calculate the distance between consecutive points
                distance = self.haversine(lat1, lon1, lat2, lon2)
                total_distance += distance
        data = [[112, 2008, total_distance]]
        #print(f"Total distance walked in 2008 by user 112: {total_distance} km")
        print(tabulate(data, headers=["User ID", "Year", "Total distance"]))


def task7_main():
    program = Task7()
    program.retrieve_total_distance_2008_id_112()
    program.connection.close_connection()      
    