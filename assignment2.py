from DbConnector import DbConnector
from tabulate import tabulate
import os
import datetime

class Trajector:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.dataset_path = "/home/alexandermoltu/Documents/H24/TDT4225/assignment_2/dataset/dataset"
        self.data = self.dataset_path + "/Data"
    
    # Insert Activity Data
    def insert_data_activity(self):
        activity_id = 0
        labeled_txt_user = {}
        
        # Load labels.txt into a dictionary for easier lookup
        for user_id in os.listdir(self.data):
            # print(user_id, os.listdir(self.data))
            user_folder = os.path.join(self.data, user_id)
            labels_path = os.path.join(user_folder, 'labels.txt')
            # print("heisann", labels_path)
            if os.path.exists(labels_path):
                with open(labels_path) as label_file:
                    for line in label_file.readlines()[1:]:  # Skip header
                        start_time, end_time, mode = line.strip().split('\t')
                        if user_id not in labeled_txt_user:
                            labeled_txt_user[user_id] = []
                        labeled_txt_user[user_id].append((start_time, end_time, mode))

        # Insert activities
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
                            # print(user_id)
                            for start_time, end_time, mode in labeled_txt_user[user_id]:
                                # print(start_time, end_time, mode)

                                print(start_time)
                                print(start_date_time)
                                print()
                                if start_time == start_date_time and end_time == end_date_time:
                                    transportation_mode = mode
                                    print(start_date_time, end_date_time, transportation_mode)

                                    break
                        
                        # Insert activity
                        activity_query = "INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_time) VALUES (%s, %s, %s, %s, %s)"
                        self.cursor.execute(activity_query, (activity_id, user_id, transportation_mode, start_date_time, end_date_time))
                        self.db_connection.commit()

                        # Insert trackpoints for the activity
                        self.insert_data_trackpoints(activity_id, lines)
                        activity_id += 1

    # Helper function to parse the datetime from a line in the .plt file
    def get_datetime_from_line(self, line):
        parts = line.strip().split(',')
        date = parts[5]  # Date part
        time = parts[6]  # Time part
        return datetime.datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S")

    # Insert TrackPoint data
    def insert_data_trackpoints(self, activity_id, lines):
        trackpoint_query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        trackpoints = []

        for line in lines:
            lat, lon, _, altitude, date_days, date_str, time_str = line.strip().split(',')
            date_time = f"{date_str} {time_str}"
            print((activity_id, float(lat), float(lon), int(altitude), float(date_days), date_time))
            trackpoints.append((activity_id, float(lat), float(lon), int(altitude), float(date_days), date_time))

        # Insert trackpoints in batch
        self.cursor.executemany(trackpoint_query, trackpoints)
        self.db_connection.commit()

    def create_table(self):
        query = ["""CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(3) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN);
                """,

                """CREATE TABLE IF NOT EXISTS Activity (
                id INT NOT NULL PRIMARY KEY,
                user_id VARCHAR(3),
                transportation_mode VARCHAR(10),
                start_date_time DATETIME,
                end_time DATETIME,
                FOREIGN KEY(user_id) REFERENCES User(id) ON DELETE CASCADE);""",

                """CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT NOT NULL PRIMARY KEY,
                activity_id INT,
                lat DOUBLE DEFAULT NULL,
                lon DOUBLE DEFAULT NULL,
                altitude INT DEFAULT NULL,
                date_days DOUBLE DEFAULT NULL,
                date_time DATETIME,
                FOREIGN KEY(activity_id) REFERENCES Activity(id) ON DELETE CASCADE);
                """
                    ]
        # This adds table_name to the %s variable and executes the query
        # self.cursor.execute(query % table_name)
        for q in query:
            self.cursor.execute(q)
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def insert_data_user(self):
        labeled_txt_user = []
        user_ids_labels = []
        
        with open(self.dataset_path + '/labeled_ids.txt') as labeled_file_user:
            labeled_txt_user = labeled_file_user.read().splitlines()[::-1]
        
        for (root,dirs,files) in os.walk(self.dataset_path, topdown=True):
            if 'labels.txt' in files:
                print(f"Found labels.txt in: {root}")
                parts = root.replace('\\', '/').split('/')
                endpart = parts[-1]
                if endpart in labeled_txt_user:
                    print(f"Right user_id {endpart}")
                    user_ids_labels.append((endpart,True))
                else: 
                    print(f"Wrong user_id {endpart}")
            else:
                parts2 = root.replace('\\', '/').split('/')
                if (len(parts2) == 7):
                    endpart = parts2[-1]
                    print(f"User Id without label {endpart}")
                    user_ids_labels.append((endpart, False))
        
        
        query = "INSERT INTO User (id, has_labels) VALUES ('%s', %s)"

        for id, has_label in user_ids_labels:
            self.cursor.execute(query % (id, has_label))
        
        self.db_connection.commit()

    def show_user_columns(self):
        self.cursor.execute("SELECT * FROM db1.User;")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        
def main():
    program = None
    try:
        program = Trajector()
        program.create_table()
        # program.insert_data_user()  # Insert User data
        program.insert_data_activity()  # Insert Activity and TrackPoint data
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
