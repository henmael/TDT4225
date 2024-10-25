from DbConnector import DbConnector

class Task8: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task8_main():
    program = Task8()
    # add relevant function(s) here
    program.connection.close_connection()

