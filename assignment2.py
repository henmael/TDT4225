from DbConnector import DbConnector
from tabulate import tabulate
import os

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

    def insert_data_user(self):
        labeled_txt_user = []
        user_ids_labels = []
        
        with open('C:/Users/henri/Desktop/dataset/labeled_ids.txt') as labeled_file_user:
            labeled_txt_user = labeled_file_user.read().splitlines()[::-1]
        
        for (root,dirs,files) in os.walk('C:/Users/henri/Desktop/dataset', topdown=True):
            if 'labels.txt' in files:
                print(f"Found labels.txt in: {root}")
                parts = root.replace('\\', '/').split('/')
                endpart = parts[-1]
                if endpart in labeled_txt_user:
                    print(f"Right user_id {endpart}")
                    user_ids_labels.append((endpart,True))
                else: 
                    print(f"Wrong user_id {endpart}")
            else:
                parts2 = root.replace('\\', '/').split('/')
                if (len(parts2) == 7):
                    endpart = parts2[-1]
                    print(f"User Id without label {endpart}")
                    user_ids_labels.append((endpart, False))
        
        
        query = "INSERT INTO User (id, has_label) VALUES ('%s', %s)"

        for id, has_label in user_ids_labels:
            self.cursor.execute(query % (id, has_label))
        
        self.db_connection.commit()


def main():
    program = None
    try:
        program = Trajector()
        #program.create_table()
        #program.show_tables()
        program.insert_data()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

