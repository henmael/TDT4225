from DbConnector import DbConnector

class Task5: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task5_main():
    program = Task5()
    # add relevant function(s) here
    program.connection.close_connection()

