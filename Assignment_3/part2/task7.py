from DbConnector import DbConnector

class Task7: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task7_main():
    program = Task7()
    # add relevant function(s) here
    program.connection.close_connection()

