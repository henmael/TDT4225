from DbConnector import DbConnector
from tabulate import tabulate


class Task4:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def retrieve_users_taxi(self):
        array = []
        query = """SELECT DISTINCT(user_id), transportation_mode FROM db1.Activity WHERE transportation_mode LIKE 'taxi';"""
        self.cursor.execute(query)
        user_activity_taxi = self.cursor.fetchall()
        
        for k, v in user_activity_taxi:
            array.append([k, v])

        print(tabulate(array, headers=["User ID", "Transportation"]))

def task4_main():
    program = Task4()
    program.retrieve_users_taxi()
    program.connection.close_connection()