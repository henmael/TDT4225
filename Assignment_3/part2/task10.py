from DbConnector import DbConnector
from pprint import pprint

class Task10: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def find_hidden_city_ids(self):
        """
        Task 10: Find users who have tracked an activity in the Forbidden City of Beijing.
        """
        pipeline = [
            { 
                "$match": {
                    "$expr": {
                        "$and": [
                            { "$gte": [{"$toDouble": "$lat"}, 39.915] },
                            { "$lt": [{"$toDouble": "$lat"}, 39.917] },
                            { "$gte": [{"$toDouble": "$lon"}, 116.396] },
                            { "$lt": [{"$toDouble": "$lon"}, 116.398] }
                        ]
                    }
                }
            },
            { 
                "$lookup": { 
                    "from": "Activity", 
                    "localField": "activities_id",  # Match to Activity by activities_id
                    "foreignField": "_id",
                    "as": "activity"
                } 
            },
            { "$unwind": "$activity" },  # Flatten array to access activity fields directly
            { "$group": { "_id": "$activity.user_id" }}  # Group by user_id to get unique users
        ]

        # Execute the aggregation pipeline
        result = list(self.db["TrackPoint"].aggregate(pipeline))
        user_ids = [doc["_id"] for doc in result]
        
        # Print the list of unique user IDs
        print("Users who tracked activities in the Forbidden City:")
        pprint(user_ids)

    def close_connection(self):
        self.connection.close_connection()

def task10_main():
    program = Task10()
    program.find_hidden_city_ids()  # Execute the task
    program.close_connection()  # Close the database connection
