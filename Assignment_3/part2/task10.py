from DbConnector import DbConnector

class Task10: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task10_main():
    program = Task10()
    # add relevant function(s) here
    program.connection.close_connection()

