from DbConnector import DbConnector
from tabulate import tabulate


class TrajectorQueries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def retrieve_amount_tables(self):
        amount_user = "SELECT COUNT(*) FROM db1.User;"
        amount_activity = "SELECT COUNT(*) FROM db1.Activity;"
        amount_trackpoint = "SELECT COUNT(*) FROM db1.TrackPoint;"

        self.cursor.execute(amount_user)
        count_user = self.cursor.fetchone()[0]

        self.cursor.execute(amount_activity)
        count_activity = self.cursor.fetchone()[0]

        self.cursor.execute(amount_trackpoint)
        count_trackpoint = self.cursor.fetchone()[0]

        table_data = [["User", count_user], ["Activity", count_activity], ["Trackpoint", count_trackpoint]]
        
        print(tabulate(table_data, headers=["Table", "Total rows"]))

if __name__ == '__main__':
    program = None
    program = TrajectorQueries()
    program.retrieve_amount_tables()
    program.connection.close_connection()