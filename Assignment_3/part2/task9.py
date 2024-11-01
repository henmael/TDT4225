from DbConnector import DbConnector
from pprint import pprint
from haversine import haversine
from datetime import datetime
from pymongo import MongoClient



class Task9: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def user_invalid_activities(self):
        # print("Query 9: Users with invalid activities:")

        pipeline_invalid_activities = [
            {
                "$sort": {
                    "activities_id": 1,
                    "date_time": 1
                }
            },
            {
                "$lookup": {
                    "from": "Activity",
                    "localField": "activities_id",
                    "foreignField": "_id",
                    "as": "activity"
                }
            },
            {
                "$unwind": "$activity"
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$activity.user_id",
                        "activities_id": "$activities_id"
                    },
                    "trackpoints": {
                        "$push": {
                            "date_time": "$date_time",
                            "_id": "$_id"
                        }
                    }
                }
            },
            {
                "$project": {
                    "deviations": {
                        "$map": {
                            "input": {"$range": [0, {"$subtract": [{"$size": "$trackpoints"}, 1]}]},
                            "as": "idx",
                            "in": {
                                "$subtract": [
                                    {"$arrayElemAt": ["$trackpoints.date_time", {"$add": ["$$idx", 1]}]},
                                    {"$arrayElemAt": ["$trackpoints.date_time", "$$idx"]}
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$match": {
                    "deviations": {
                        "$elemMatch": {
                            "$gte": 5 * 60  
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "invalid_activities_count": {
                        "$sum": 1
                    }
                }
            },
            {
                "$project": {
                    "user_id": "$_id",
                    "invalid_activities_count": 1,
                    "_id": 0
                }
            }
        ]

        invalid_activities = list(self.db["TrackPoint"].aggregate(pipeline_invalid_activities))
        # print(invalid_activities)

        # print("Total: ", str(len(invalid_activities)), " activities")
        # result = list(self.db["Activity"].aggregate(pipeline))
        # print("User with the most invalid activities:")
        pprint(invalid_activities)



def task9_main():
    program = Task9()
    program.user_invalid_activities()
    program.connection.close_connection()

