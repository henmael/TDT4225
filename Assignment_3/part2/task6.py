from DbConnector import DbConnector

class Task6: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task6_main():
    program = Task6()
    # add relevant function(s) here
    program.connection.close_connection()

