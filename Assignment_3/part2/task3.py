from DbConnector import DbConnector
# Find the top 20 users with the highest number of activities
class Task3: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

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
        print("-------------------------------------------")
        for index, user in enumerate(activity_per_user, start=1):
            print(f"{index} UserID: {user['_id']}   |   Amount of activities: {user['activity_count']}")


def task3_main():
    program = Task3()
    program.top_20_users_highest_activities("Activity")
    program.connection.close_connection()

