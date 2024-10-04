from tabulate import tabulate
from DbConnector import DbConnector


class Task11:
    def __init__(self): 
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def retrieve_all_users_with_transp_mode_and_most_used_mode(self):
        array = []
        query = """SELECT user_id, transportation_mode as most_used_transportation_mode
                   FROM (
                      SELECT A.user_id, A.transportation_mode, 
                         ROW_NUMBER() OVER (PARTITION BY A.user_id ORDER BY COUNT(A.transportation_mode) DESC) AS mode_rank
                         FROM Activity A
                         WHERE A.transportation_mode IS NOT NULL
                         GROUP BY A.user_id, A.transportation_mode
                         ) AS RankedModes
                         WHERE mode_rank = 1
                         ORDER BY user_id;"""
        self.cursor.execute(query)
        test = self.cursor.fetchall()
        
        for k, v in test:
            array.append([k, v])

        print(tabulate(array, headers=["User ID", "Most used transportation mode"]))

def task11_main():
    program = Task11()
    program.retrieve_all_users_with_transp_mode_and_most_used_mode()
    program.connection.close_connection()
