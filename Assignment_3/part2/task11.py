from DbConnector import DbConnector

class Task11: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task11_main():
    program = Task11()
    # add relevant function(s) here
    program.connection.close_connection()

