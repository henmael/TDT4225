from DbConnector import DbConnector
from tabulate import tabulate

class Task10:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        

    def retrieve_users_at_forbidden_city(self):
        query = """
        SELECT DISTINCT u.id AS user_id
        FROM User u
        JOIN Activity a ON u.id = a.user_id
        JOIN TrackPoint tp ON a.id = tp.activity_id
        WHERE tp.lat BETWEEN 39.915 AND 39.917
        AND tp.lon BETWEEN 116.396 AND 116.398;
        """
        print("Executing query to find users who have tracked activities in the Forbidden City...")
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        # Display the result in a tabulated format
        print(tabulate(result, headers=["User ID"]))

def task10_main():
    program = Task10()
    # Find users at the Forbidden City
    program.retrieve_users_at_forbidden_city()
    # Close the connection
    program.connection.close_connection()