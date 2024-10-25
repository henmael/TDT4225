from DbConnector import DbConnector

class Task3: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    # Find the top 20 users with the highest number of activities
    def top_20_users_highest_activities(self, collection_name):
        collection = self.db[collection_name]
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': None}
                }
            },
            {
                '$group': {
                    '_id': '$user_id',
                    'activity_count': {'$sum': 1}
                }
            },
            {
                '$sort': {'activity_count': -1}
            },
            {
                '$limit': 20,
            }
        ]
        activity_per_user = collection.aggregate(pipeline)
        print("Top 20 users with highest activity count")
        for i in activity_per_user:
            print(f"UserID: {i['_id']}")


def task3_main():
    program = Task3()
    program.top_20_users_highest_activities("Activity")
    program.connection.close_connection()

