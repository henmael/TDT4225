import pprint
from DbConnector import DbConnector

# Find all types of transportation modes and count how many activities 
    # that are tagged with these transportation mode labels. Do not count the rows where 
    # the mode is null.
class Task5: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def amount_activities_per_transportation(self, collection_name):
        collection = self.db[collection_name]
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': None}
                }
            },{
                '$group': {
                    '_id': '$transportation_mode',
                    'count': {
                        '$sum': 1
                    }
                }
            }
        ]

        activity_per_user = collection.aggregate(pipeline)

        for transportation_activities in activity_per_user:
            formatted_activity = {
                'activity': transportation_activities['_id'],
                'sum': transportation_activities['count']
            }
            pprint.pp(formatted_activity)


def task5_main():
    program = Task5()
    program.amount_activities_per_transportation('Activity')
    program.connection.close_connection()

