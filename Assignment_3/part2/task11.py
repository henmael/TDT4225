import pprint
from DbConnector import DbConnector
# Find all users who have registered transportation_mode and their most used transportation_mode.
class Task11: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def registered_transportation_most_used_transportation(self, collection_name):
        # The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id. --- this is also done
        # Some users may have the same number of activities tagged with e.g. walk and car.  --- its also done
            #  In this case it is up to you to decide which transportation mode to include in your answer (choose one). 
        # Do not count the rows where the mode is null. ----- its done
        collection = self.db[collection_name]
        pipeline = [
            {
                '$match': {
                    'transportation_mode': {'$ne': None}
                }
            },
            {
        '$group': {
            '_id': {
                'user_id': '$user_id',
                'transport_mode': '$transportation_mode'
            },
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    },
    {
        '$group': {
            '_id': '$_id.user_id',
            'most_common_transport': {'$first': '$_id.transport_mode'},
            'count': {'$first': '$count'}
        }
    }
        ]
        test = collection.aggregate(pipeline)
        t = sorted(test, key=lambda x: x['_id'])
        print("Users who have registered transportation_mode and their most used transportation_mode: ")
        print("----------------------------------------------------------------------------------------------")
        for transportation_activities_user in t: 
            formatted_trackpoint = {
                'user_id': transportation_activities_user['_id'],
                'most_used_transportation_mode': transportation_activities_user['most_common_transport']
            }
            pprint.pp(formatted_trackpoint)

def task11_main():
    program = Task11()
    program.registered_transportation_most_used_transportation("Activity")
    program.connection.close_connection()

