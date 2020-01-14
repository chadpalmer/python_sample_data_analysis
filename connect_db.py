import sqlite3
from sqlite3 import Error


class ConnectDB:

    def __init__(self):
        pass


    @staticmethod
    def connect_db(db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            # print(sqlite3.version)
            return conn
        except Error as e:
            print(e)

        return conn


    @staticmethod
    def create_table(conn, create_table_sql):
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
            return True
        except Error as e:
            print(e)
            return False

    @staticmethod
    def insert_row_into_table(conn, str_sql, row_tuple):
        cur = conn.cursor()
        cur.execute(str_sql, row_tuple)
        conn.commit()
        return cur.lastrowid

    @staticmethod
    def select_rows_by_query(conn, str_sql):
        cur = conn.cursor()
        cur.execute(str_sql)
        return cur.fetchall()

    @staticmethod
    def close_connection(conn):
        conn.close()
