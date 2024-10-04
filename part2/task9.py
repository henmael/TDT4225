from DbConnector import DbConnector
from tabulate import tabulate

class Task9:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def retrieve_users_taxi(self):
        array = []
        query = """

            SELECT U.id, COUNT(DISTINCT A.id) AS invalid_activity_count
            FROM (
                SELECT TP1.activity_id
                FROM TrackPoint TP1
                JOIN TrackPoint TP2 ON TP1.activity_id = TP2.activity_id
                                    AND TP1.id = TP2.id + 1
                WHERE TIMESTAMPDIFF(SECOND, TP2.date_time, TP1.date_time) >= 300
            ) AS InvalidActivities
            JOIN Activity A ON InvalidActivities.activity_id = A.id
            JOIN User U ON A.user_id = U.id
            GROUP BY U.id;

        """

        self.cursor.execute(query)
        user_activity_taxi = self.cursor.fetchall()
        
        for k, v in user_activity_taxi:
            array.append([k, v])

        print(tabulate(array, headers=["User ID", "Transportation"]))

def task9_main():
    program = Task9()
    program.retrieve_users_taxi()
    program.connection.close_connection()