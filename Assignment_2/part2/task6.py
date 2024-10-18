from DbConnector import DbConnector
from tabulate import tabulate

class Task6:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def retrieve_most_active_year(self):
        query = """
        SELECT YEAR(start_date_time) AS activity_year, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY activity_year
        ORDER BY activity_count DESC
        LIMIT 1;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        # Display the result in a tabulated format
        print(tabulate(result, headers=["Year", "Activity Count"]))

    def retrieve_year_most_hours(self):
        query = """
        SELECT YEAR(start_date_time) AS activity_year, 
               SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_time)) AS total_hours
        FROM Activity
        WHERE end_time IS NOT NULL
        GROUP BY activity_year
        ORDER BY total_hours DESC
        LIMIT 1;
        """
        print("Executing query to find year with most recorded hours...")
        self.cursor.execute(query)
        result = self.cursor.fetchall()

        # Display the result in a tabulated format
        print(tabulate(result, headers=["Year", "Total Recorded Hours"]))

def task6_main():
    program = Task6()

    # Part a: Find the year with the most activities
    program.retrieve_most_active_year()

    # Part b: Find the year with the most recorded hours
    program.retrieve_year_most_hours()

    # Close the connection
    program.connection.close_connection()
