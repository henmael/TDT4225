from DbConnector import DbConnector
from tabulate import tabulate

class Trajector:
    
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def create_table(self):
        query = ["""CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(3) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN);
                """,

                """CREATE TABLE IF NOT EXISTS Activity (
                id INT NOT NULL PRIMARY KEY,
                user_id VARCHAR(3),
                transportation_mode VARCHAR(10),
                start_date_time DATETIME,
                end_time DATETIME,
                FOREIGN KEY(user_id) REFERENCES User(id) ON DELETE CASCADE);""",

                """CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT NOT NULL PRIMARY KEY,
                activity_id INT,
                lat DOUBLE DEFAULT NULL,
                lon DOUBLE DEFAULT NULL,
                altitude INT DEFAULT NULL,
                date_days DOUBLE DEFAULT NULL,
                date_time DATETIME,
                FOREIGN KEY(activity_id) REFERENCES Activity(id) ON DELETE CASCADE);
                """
                    ]
        # This adds table_name to the %s variable and executes the query
        # self.cursor.execute(query % table_name)
        for q in query:
            self.cursor.execute(q)
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = Trajector()
        program.create_table()
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

