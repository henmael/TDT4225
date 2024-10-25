from DbConnector import DbConnector

class Task9: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task9_main():
    program = Task9()
    # add relevant function(s) here
    program.connection.close_connection()

