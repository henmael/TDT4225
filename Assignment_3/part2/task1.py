from DbConnector import DbConnector
# How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
class Task1: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_amount_collections(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.count_documents({})
        print(f"{collection_name} : {documents}\n")

    def fetch_users_activities_trackpoints_amounts(self):
        print("How many users, activities and trackpoints are there in the dataset: ")
        print("--------------------------------------------------------------------")
        self.fetch_amount_collections("User")
        self.fetch_amount_collections("Activity")
        self.fetch_amount_collections("TrackPoint")

def task1_main():
    program = Task1()
    program.fetch_users_activities_trackpoints_amounts()
    program.connection.close_connection()

