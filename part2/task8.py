from DbConnector import DbConnector
from tabulate import tabulate

class Task8:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def retrieve_users_taxi(self):
        array = []
        query = """
            SELECT U.id, SUM(TP.altitude_diff) AS total_gain
            FROM (
                SELECT TP1.activity_id, (TP1.altitude - TP2.altitude) * 0.3048 AS altitude_diff
                FROM TrackPoint TP1
                JOIN TrackPoint TP2 ON TP1.activity_id = TP2.activity_id
                                    AND TP1.id = TP2.id + 1
                WHERE TP1.altitude > TP2.altitude
                AND TP1.altitude != -777
                AND TP2.altitude != -777
            ) AS TP
            JOIN Activity A ON TP.activity_id = A.id
            JOIN User U ON A.user_id = U.id
            GROUP BY U.id
            ORDER BY total_gain DESC
            LIMIT 20;
        """

        self.cursor.execute(query)
        user_activity_taxi = self.cursor.fetchall()
        
        for k, v in user_activity_taxi:
            array.append([k, v])

        print(tabulate(array, headers=["User ID", "Total altidude gained (meters)"]))

def task8_main():
    program = Task8()
    program.retrieve_users_taxi()
    program.connection.close_connection()