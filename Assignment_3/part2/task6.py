from DbConnector import DbConnector
from pprint import pprint

class Task6: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def year_with_most_activities(self):
        """
        Task 6a: Find the year with the most activities.
        """
        pipeline = [
            {
                "$group": {
                    "_id": { "$year": "$start_date_time" },
                    "activity_count": { "$sum": 1 }
                }
            },
            { "$sort": { "activity_count": -1 } },
            { "$limit": 1 }
        ]
        
        result = list(self.db["Activity"].aggregate(pipeline))
        year_with_most_activities = result[0] if result else None
        print("Year with the most activities:")
        pprint(year_with_most_activities)
        return year_with_most_activities["_id"] if year_with_most_activities else None

    def year_with_most_recorded_hours(self):
        """
        Task 6b: Find the year with the most recorded hours.
        """
        pipeline = [
            {
                "$project": {
                    "year": { "$year": "$start_date_time" },
                    "duration": { "$subtract": ["$end_date_time", "$start_date_time"] }
                }
            },
            {
                "$group": {
                    "_id": "$year",
                    "total_duration": { "$sum": "$duration" }
                }
            },
            { "$sort": { "total_duration": -1 } },
            { "$limit": 1 }
        ]
        
        result = list(self.db["Activity"].aggregate(pipeline))
        year_with_most_hours = result[0] if result else None
        print("Year with the most recorded hours:")
        pprint(year_with_most_hours)
        return year_with_most_hours["_id"] if year_with_most_hours else None
    
    def close_connection(self):
        self.connection.close_connection()

def task6_main():
    program = Task6()
    
    # Execute the tasks to find the year with most activities and most recorded hours
    most_activities_year = program.year_with_most_activities()
    most_hours_year = program.year_with_most_recorded_hours()
    
    # Compare and print results
    if most_activities_year == most_hours_year:
        print(f"The year {most_activities_year} has both the most activities and the most recorded hours.")
    else:
        print(f"The year with the most activities is {most_activities_year}, while the year with the most recorded hours is {most_hours_year}.")
    
    # Close the database connection
    program.close_connection()


