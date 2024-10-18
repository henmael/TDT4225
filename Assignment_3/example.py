from pprint import pprint 
from DbConnector import DbConnector
import os
import time
from datetime import datetime
from pathlib import Path
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from tabulate import tabulate
from haversine import haversine



class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        #self.cursor = self.connection.cursor
        self.counter_ignored = 0
        self.counter_trackpoints = 0
        self.counter_transportation = 0
        self.counter_transportation_ignored = 0
        self.dataset = ""

    def create_colls(self, coll_names):

        # Create the collections
        for coll_name in coll_names:
            self.db.create_collection(coll_name)
            print('Created collection: ', coll_name)

    def insert_into_user(self, sorted_users):

        collection = self.db["User"]
        collection.insert_many(sorted_users)


    def insert_into_activity(self, activities):
        
        collection = self.db["Activity"]
        collection.insert_many(activities)

    def insert_transportation_into_activity(self, labeled_activities):
    
        collection = self.db["Activity"]
        # bulk = collection.initialize_unordered_bulk_op()
        bulk_operation_list = []

        for activity in labeled_activities:
            # user_id, transportation_mode, start_date_time, end_date_time = activity
            user_id = activity.get("user_id")
            start_date_time = activity.get("start_date_time")
            end_date_time = activity.get("end_date_time")
            # Fetch activities with the same user_id, start_date_time and end_date_time
            # matching_activity = collection.update_one({
            #     "user_id": user_id,
            #     "start_date_time": start_date_time,
            #     "end_date_time": end_date_time
            # }, {
            #     "$set": {"transportation_mode": activity.get("transportation_mode")}
            # })
            
            # # 0 count means no match was found
            # if matching_activity.matched_count == 0:
            #     self.counter_transportation_ignored += 1
            # else:
            #     self.counter_transportation += 1
            
            bulk_operation_list.append(UpdateOne({
                "user_id": user_id,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time
            }, {
                "$set": {"transportation_mode": activity.get("transportation_mode")}
            }))
            
        result = collection.bulk_write(bulk_operation_list)
        print("updated " + str(result.modified_count) + " labeled activities")

    def insert_trackpoints(self, trackpoints):
        collection = self.db["TrackPoint"]  

        n = 500000
        splicedTrackpoints = [trackpoints[i:i + n] for i in range(0, len(trackpoints), n)]
        size = len(splicedTrackpoints)
        print("spliced trackpoints into " + str(size) + " chunks")

        for index, chunk in enumerate(splicedTrackpoints):
            try:
                collection.insert_many(chunk, ordered=False)
            except BulkWriteError as bwe:
                print(bwe.details)
            print(size - index, "chunks left")


    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_colls(self, coll_names):

         # Drop the collections
        for coll_name in coll_names:
            collection = self.db[coll_name]
            print(collection)
            print()
            collection.drop()

        
    def show_coll(self):
        collections = self.db.list_collection_names()
        print(collections)

    def insert_data_into_mongo_db(self):

         # Read the ids from labeled_ids.txt and store ids in an array;
        ids = []
        # dataset = "../../dataset/dataset/"
        try:
            with open(self.dataset + 'labeled_ids.txt', 'r') as file:
                # Iterate over each line in the file
                print('opening ' + self.dataset + 'labeled_ids.txt')
                for line in file:
                    # Strip white space and add to ids list
                    ids.append(line.strip())
        except FileNotFoundError:
            print("The file data.txt was not found.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
         
        # root_dir = os.getcwd()
        users = []
        activities = []
        activities_id = 0
        trackpoints = []
        print("Dataset dir: " + self.dataset)
        for foldername, _, filenames in os.walk(self.dataset + "/Data"):
            
                # Check if the foldername ends with "Trajectory", if not, it is a user folder
                if not foldername.endswith("Trajectory") and not foldername.endswith("Data"):
                    id = foldername[-3:]
                    # Flag and append the user with has_labels if the id is in the id's list
                    if id in ids:
                        print(id, "has labels")
                        users.append(
                            {
                                "_id": id,
                                "has_labels": 1
                            }
                        )

                    # Flag and append the user with has_labels if the id is not in the id's list
                    else:
                        print(id, "not labels")

                        users.append(
                            {
                                "_id": id,
                                "has_labels": 0
                            }
                        )
                
                # This for loop populates the users, activities and trackpoints lists with values from the .plt files
                for file in filenames:
                    # only handle plt files
                    if file.endswith("plt"):
                        user_id = foldername[-14:-11]
                        with open(foldername + '/' + file) as file:
                            # Check if the plt file contains fewer or exactly 2500 lines
                            # 2506 since first 6 lines is the header
                            file = file.readlines()
                            if len(file) <= 2506:
                                for i, line in enumerate(file):
                                    # ignore first 6 lines, rest is added to trackpoints array
                                    if i > 6:
                                        
                                        columns = line.strip().split(',')
                                        lat, lon, alt, date_days, date, the_time = columns[0], columns[1], columns[3], columns[4], columns[5], columns[6]
                                        date_time = date + " " + the_time
                                        # This returns an error if a date does not match the correct format,
                                        # so no corrupted data is added to the database
                                        date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
                                        trackpoints.append({
                                            "_id": self.counter_trackpoints,
                                            "activities_id": activities_id,
                                            "lat": lat,
                                            "lon": lon,
                                            "alt": alt,
                                            "date_days": date_days,
                                            "date_time": date_time
                                        })
                                        self.counter_trackpoints += 1
                                        # print("added trackpoint: " + str(activities_id) + " " + str(lat) + " " + str(lon) + " " + str(alt) + " " + str(date_days) + " " + str(date_time))
                                    # The seventh line (index 6) is the first valid line in the file, and contains the start date and time of the activity
                                    if i == 6:
                                        columns = line.strip().split(',')
                                        start_date, start_time = columns[5], columns[6]
                                        start_date_time_str = start_date + " " + start_time
                                        
                                    # Last line gives the end date and time of the activity. The activity is then appended to the activities list
                                    if i == len(file) - 1:
                                        columns_lastline = line.strip().split(',')
                                        end_date, end_time = columns_lastline[5], columns_lastline[6]
                                        end_date_time_str = end_date + " " + end_time
                                        end_date_time = datetime.strptime(end_date_time_str, '%Y-%m-%d %H:%M:%S')
                                        start_date_time = datetime.strptime(start_date_time_str, '%Y-%m-%d %H:%M:%S')

                                        activities.append({
                                            "_id" : activities_id,
                                            "user_id":   user_id,
                                            "transportation_mode": None, 
                                            "start_date_time":  start_date_time,
                                            "end_date_time": end_date_time
                                        })
                                        activities_id += 1
                            else:
                                self.counter_ignored += 1

        sorted_users = sorted(users, key=lambda x: x['_id'])
        tik = time.time()
        self.insert_into_user(sorted_users)
        tok = time.time()
        print("finished inserting users after: " + str(tok - tik) + " seconds.")

        print("starting inserting activities into db...")
        tik = time.time()
        self.insert_into_activity(activities)
        tok = time.time()
        print("finished inserting activities after: " + str(tok - tik) + " seconds.")

        labeled_activities = []
        for foldername, _, filenames in os.walk(self.dataset + "/Data"):
            for file in filenames:
                # only handle txt files
                if file.endswith("txt"):
                    user_id = foldername[-3:]
                    with open(foldername + '/' + file) as file:
                        for i, line in enumerate(file):
                            # Skip first line
                            if i == 0:
                                continue
                            line = line.strip().split('\t')
                            start_date_time_str, end_date_time_str, transportation_mode = line[0], line[1], line[2]
                            start_date_time = datetime.strptime(start_date_time_str, '%Y/%m/%d %H:%M:%S')
                            end_date_time = datetime.strptime(end_date_time_str, '%Y/%m/%d %H:%M:%S')
                            #labeled_activities.append((user_id, transportation_mode, start_date_time, end_date_time))
                            labeled_activities.append({
                                            "user_id":   user_id,
                                            "transportation_mode": transportation_mode, 
                                            "start_date_time":  start_date_time,
                                            "end_date_time": end_date_time
                                        })
        print("inserting transportation into activities...")
        tik = time.time()
        self.insert_transportation_into_activity(labeled_activities)
        tok = time.time()
        print("finished inserting transportation after: " + str(tok - tik) + " seconds.")

        print("inserting trackpoints into db...")
        tik = time.time()
        self.insert_trackpoints(trackpoints)
        tok = time.time()
        print("finished inserting trackpoints after: " + str(tok - tik) + " seconds.")
        
        return



def main():
    program = None
    try:
        program = ExampleProgram()

        program.drop_colls(["User", "Activity", "TrackPoint"])
        program.create_colls(["User", "Activity", "TrackPoint"])
        # program.insert_data_into_mongo_db()
        program.show_coll()


        # program.insert_documents(collection_name="Person")
        # program.fetch_documents(collection_name="Person")
        # program.drop_colls(["User", "Activity", "TrackPoint"])
        # program.drop_colls(collection_name='person')
        # program.drop_colls(collection_name='users')
        # Check that the table is dropped
        # program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
