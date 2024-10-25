from DbConnector import DbConnector

class Task4: 
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db


def task4_main():
    program = Task4()
    # add relevant function(s) here
    program.connection.close_connection()

