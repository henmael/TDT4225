from DbConnector import DbConnector

class Task8: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def top_20_users_gained_altitude(self):
        """
        Find the top 20 users who have gained the most altitude.
        """
        pipeline = [
            { "$match": { "alt": {"$ne": "-777"} } },  # Exclude invalid altitude values
            { "$addFields": { "altitude_numeric": { "$toDouble": "$alt" } } },  # Convert alt from string to double
            { 
                "$sort": { "activities_id": 1, "date_time": 1 }  # Sort by activity and time
            },
            { 
                "$lookup": { 
                    "from": "Activity",
                    "localField": "activities_id",
                    "foreignField": "_id",
                    "as": "activity"
                } 
            },
            { "$unwind": "$activity" },  # Flatten to access user_id directly
            { 
                "$group": {
                    "_id": "$activity.user_id",
                    "altitude_points": { "$push": "$altitude_numeric" }
                }
            },
            {
                "$project": {
                    "total_altitude_gain": {
                        "$sum": {
                            "$map": {
                                "input": { "$range": [1, { "$size": "$altitude_points" }] },
                                "as": "index",
                                "in": {
                                    "$cond": [
                                        { "$gt": [
                                            { "$subtract": [
                                                { "$arrayElemAt": ["$altitude_points", "$$index"] },
                                                { "$arrayElemAt": ["$altitude_points", { "$subtract": ["$$index", 1] }] }
                                            ]}, 
                                            0
                                        ]},
                                        { "$subtract": [
                                            { "$arrayElemAt": ["$altitude_points", "$$index"] },
                                            { "$arrayElemAt": ["$altitude_points", { "$subtract": ["$$index", 1] }] }
                                        ]},
                                        0
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            { "$sort": { "total_altitude_gain": -1 } },  # Sort by total altitude gain
            { "$limit": 20 }  # Limit to top 20 users
        ]

        result = list(self.db["TrackPoint"].aggregate(pipeline))
        print("Top 20 users who have gained the most altitude:")
        for user in result:
            print(f"User ID: {user['_id']}, Total Altitude Gain: {user['total_altitude_gain']} meters")

    def close_connection(self):
        self.connection.close_connection()

def task8_main():
    program = Task8()
    program.top_20_users_gained_altitude()
    program.close_connection()


