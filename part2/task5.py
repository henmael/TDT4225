from DbConnector import DbConnector
from tabulate import tabulate

class Task5:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def retrieve_users_taxi(self):
        array = []
        query = """
            SELECT transportation_mode, COUNT(*) AS mode_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY transportation_mode
            ORDER BY mode_count DESC;
        """

        self.cursor.execute(query)
        user_activity_taxi = self.cursor.fetchall()
        
        for k, v in user_activity_taxi:
            array.append([k, v])

        print(tabulate(array, headers=["User ID", "Transportation"]))

def task5_main():
    program = Task5()
    program.retrieve_users_taxi()
    program.connection.close_connection()