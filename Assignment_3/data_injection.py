import os
import datetime
from DbConnector import DbConnector

class Trajector:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.dataset_path = ""
        self.data = os.path.join(self.dataset_path, "Data")
    
    # Insert Activity Data into MongoDB
    def insert_data_activity(self):
        labeled_txt_user = {}
        activity_id = 0

        # Load labels.txt into a dictionary for easier lookup
        for user_id in os.listdir(self.data):
            user_folder = os.path.join(self.data, user_id)
            labels_path = os.path.join(user_folder, 'labels.txt')
            if os.path.exists(labels_path):
                with open(labels_path) as label_file:
                    for line in label_file.readlines()[1:]:  # Skip header
                        start_time, end_time, mode = line.strip().split('\t')
                        start_time = start_time.replace('/', '-')  
                        start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                        end_time = datetime.datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")

                        if user_id not in labeled_txt_user:
                            labeled_txt_user[user_id] = []
                        labeled_txt_user[user_id].append((start_time, end_time, mode))

        # Insert activities into MongoDB
        activities = []
        trackpoints = []
        
        for user_id in os.listdir(self.data):
            trajectory_folder = os.path.join(self.data, user_id, 'Trajectory')
            if not os.path.exists(trajectory_folder):
                continue
            
            for file in os.listdir(trajectory_folder):
                if file.endswith('.plt'):
                    plt_file_path = os.path.join(trajectory_folder, file)
                    with open(plt_file_path) as plt_file:
                        lines = plt_file.readlines()[6:]  # Skip the first 6 header lines

                        # Skip files with more than 2500 trackpoints
                        if len(lines) > 2500:
                            continue

                        start_date_time = self.get_datetime_from_line(lines[0])
                        end_date_time = self.get_datetime_from_line(lines[-1])

                        # Check if transportation mode is available in labels
                        transportation_mode = None
                        if user_id in labeled_txt_user:
                            for start_time, end_time, mode in labeled_txt_user[user_id]:
                                if start_time == start_date_time and end_time == end_date_time:
                                    transportation_mode = mode
                                    break

                        # Create an activity document
                        activity = {
                            "_id": activity_id,
                            "user_id": user_id,
                            "transportation_mode": transportation_mode,
                            "start_date_time": start_date_time,
                            "end_date_time": end_date_time
                        }
                        activities.append(activity)

                        # Insert TrackPoints
                        for line in lines:
                            lat, lon, _, altitude, date_days, date_str, time_str = line.strip().split(',')
                            date_time = f"{date_str} {time_str}"
                            trackpoint = {
                                "activity_id": activity_id,
                                "lat": float(lat),
                                "lon": float(lon),
                                "altitude": int(float(altitude)),
                                "date_days": float(date_days),
                                "date_time": date_time
                            }
                            trackpoints.append(trackpoint)
                        
                        activity_id += 1
        
        # Insert activities and trackpoints into MongoDB collections
        self.db["Activity"].insert_many(activities)
        self.db["TrackPoint"].insert_many(trackpoints)
        print(f"Inserted {len(activities)} activities and {len(trackpoints)} trackpoints.")

    # Helper function to parse the datetime from a line in the .plt file
    def get_datetime_from_line(self, line):
        parts = line.strip().split(',')
        date = parts[5]  # Date part
        time = parts[6]  # Time part
        return datetime.datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S")

    # Insert User data into MongoDB
    def insert_data_user(self):
        labeled_txt_user = []
        users = []
        
        # Load labeled_ids.txt
        with open(self.dataset_path + '/labeled_ids.txt') as labeled_file_user:
            labeled_txt_user = labeled_file_user.read().splitlines()[::-1]
        
        # Walk through the dataset folder to find users
        for root, dirs, files in os.walk(self.dataset_path, topdown=True):
            if 'labels.txt' in files:
                user_id = root.split('/')[-1]
                has_labels = user_id in labeled_txt_user
                users.append({"_id": user_id, "has_labels": has_labels})
        
        # Insert user data into MongoDB
        self.db["User"].insert_many(users)
        print(f"Inserted {len(users)} users.")

def trajector_main():
    program = None
    try:
        program = Trajector()
        program.insert_data_user()  # Insert User data
        program.insert_data_activity()  # Insert Activity and TrackPoint data
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()
