from DbConnector import DbConnector
from tabulate import tabulate

# Find the top 20 users with the highest number of activities.
class Task3:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def retrieve_top_20_users_activities(self):
        array = []
        query = """SELECT user_id, COUNT(user_id) 
                    FROM db1.Activity
                    GROUP BY user_id 
                    ORDER BY COUNT(user_id) DESC
                    LIMIT 20;"""
        self.cursor.execute(query)
        all = self.cursor.fetchall()

        for k, v in all:
            array.append([k, v])
        print(tabulate(array, headers=["User ID", "No. Activities"]))

    
def task3_main():
    program = Task3()
    program.retrieve_top_20_users_activities()
    program.connection.close_connection()
