from pprint import pprint
from DbConnector import DbConnector
from haversine import haversine
from datetime import datetime
from pymongo import MongoClient

class MongoDBQueries:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    def count_documents(self):
        """
        Task 1: How many users, activities, and trackpoints are there in the dataset?
        """
        user_count = self.db["User"].count_documents({})
        activity_count = self.db["Activity"].count_documents({})
        trackpoint_count = self.db["TrackPoint"].count_documents({})
        print("Number of users:", user_count)
        print("Number of activities:", activity_count)
        print("Number of trackpoints:", trackpoint_count)

    def average_activities_per_user(self):
        """
        Task 2: Find the average number of activities per user.
        """
        user_count = self.db["User"].count_documents({})
        activity_count = self.db["Activity"].count_documents({})
        average = activity_count / user_count if user_count > 0 else 0
        print("Average number of activities per user:", average)

    def top_20_users_highest_activities(self):
        """
        Task 3: Find the top 20 users with the highest number of activities.
        """
        pipeline = [
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 20}
        ]
        top_users = list(self.db["Activity"].aggregate(pipeline))
        print("Top 20 users with the highest number of activities:")
        pprint(top_users)

    def users_taken_taxi(self):
        """
        Task 4: Find all users who have taken a taxi.
        """
        users = self.db["Activity"].distinct("user_id", {"transportation_mode": "taxi"})
        print("Users who have taken a taxi:")
        pprint(users)

    def count_transportation_modes(self):
        """
        Task 5: Count how many activities are tagged with each transportation mode.
        """
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}}
        ]
        transportation_counts = list(self.db["Activity"].aggregate(pipeline))
        print("Transportation mode counts:")
        pprint(transportation_counts)

    def year_with_most_activities(self):
        """
        Task 6a: Find the year with the most activities.
        """
        pipeline = [
            {"$group": {"_id": {"$year": "$start_date_time"}, "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 1}
        ]
        result = list(self.db["Activity"].aggregate(pipeline))
        print("Year with the most activities:")
        pprint(result)

    def year_with_most_recorded_hours(self):
        """
        Task 6b: Is this also the year with the most recorded hours?
        """
        pipeline = [
            {"$project": {"year": {"$year": "$start_date_time"}, "duration": {"$subtract": ["$end_date_time", "$start_date_time"]}}},
            {"$group": {"_id": "$year", "total_duration": {"$sum": "$duration"}}},
            {"$sort": {"total_duration": -1}},
            {"$limit": 1}
        ]
        result = list(self.db["Activity"].aggregate(pipeline))
        print("Year with the most recorded hours:")
        pprint(result)

    def total_distance_walked_2008(self, user_id):
        """
        Task 7: Find the total distance (in km) walked in 2008 by user with id=112.
        """
        trackpoints = list(self.db["TrackPoint"].find({
            "activities_id": user_id,
            "$expr": {"$eq": [{"$year": "$date_time"}, 2008]}
        }).sort("date_time", 1))

        total_distance = 0.0
        for i in range(1, len(trackpoints)):
            prev_point = trackpoints[i - 1]
            curr_point = trackpoints[i]
            total_distance += haversine((prev_point["lat"], prev_point["lon"]), (curr_point["lat"], curr_point["lon"]))

        print(f"Total distance walked in 2008 by user {user_id}: {total_distance} km")

    def close_connection(self):
        self.connection.close_connection()


def main():
    queries = MongoDBQueries()
    try:
        queries.count_documents()
        queries.average_activities_per_user()
        queries.top_20_users_highest_activities()
        queries.users_taken_taxi()
        queries.count_transportation_modes()
        queries.year_with_most_activities()
        queries.year_with_most_recorded_hours()
        queries.total_distance_walked_2008(user_id=112)
    finally:
        queries.close_connection()


if __name__ == "__main__":
    main()
