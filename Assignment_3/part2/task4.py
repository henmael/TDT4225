from DbConnector import DbConnector
# Find all users who have taken a taxi.
class Task4: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def users_taken_taxi(self, collection_name):
        collection = self.db[collection_name]
        pipeline = [
            {
                '$match': {
                    'transportation_mode': 'taxi'
                }
            },
            {
                '$group': {
                    '_id': '$user_id',
                    'transportation_mode': {'$first': '$transportation_mode'}
                }
            },

        ]

        users_taxi = collection.aggregate(pipeline)
        test = sorted(users_taxi, key=lambda x: x['_id'])

        print("Users who have taken a taxi: ")
        print("-----------------------------")
        for i in test: 
            print(f"UserID: {i['_id']}  |   Activity: {i['transportation_mode']}")


def task4_main():
    program = Task4()
    program.users_taken_taxi("Activity")
    program.connection.close_connection()

