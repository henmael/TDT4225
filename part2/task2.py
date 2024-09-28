from decimal import Decimal
from DbConnector import DbConnector
from tabulate import tabulate

class Task2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def average_activities_per_user(self):
        query = """SELECT AVG(count_acitivity) FROM (
        SELECT COUNT(*) as count_acitivity FROM db1.Activity
        GROUP BY user_id) as user_activities;"""
        self.cursor.execute(query)
        average = self.cursor.fetchone()[0]

        table_data = [["Average activities (ca)", "Average activities (full value)"], [round(average), average]]
        print(tabulate(table_data, headers="firstrow"))

def task2_main():
    program = Task2()
    program.average_activities_per_user()
    program.connection.close_connection()