from DbConnector import DbConnector
# Find the average number of activities per user.
class Task2: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def average_activities_per_user(self, collection_name):
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
            }
        ]

        activity_per_user = collection.aggregate(pipeline)

        p = 0
        result_pluss = 0
        i = 0
        for t in activity_per_user:
            p += t['activity_count'] 
            result_pluss = p
            i+=1

        print("Find the average number of activities per user: ")
        print("------------------------------------------------")
        average = result_pluss / i
        print(f"Average activity per user: {round(average, 4)}")


def task2_main():
    program = Task2()
    program.average_activities_per_user("Activity")
    program.connection.close_connection()

