from DbConnector import DbConnector

class Task2: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def average_activities_per_user(self, collection_name):
        collection = self.db[collection_name]
        #users_amount_with_activity = collection.count_documents({'has_labels': 0}) # right now 0 for testing
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

        average = result_pluss / i
        print(f"Average activity per user: {round(average, 4)}")


def task2_main():
    program = Task2()
    program.average_activities_per_user("Activity")
    program.connection.close_connection()

