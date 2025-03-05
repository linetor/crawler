import psycopg2
from psycopg2 import pool

class PostgresSingleton:
    _instance = None
    _connection_pool = None

    def __new__(cls, db_config):
        if cls._instance is None:
            cls._instance = super(PostgresSingleton, cls).__new__(cls)
            cls._connection_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                user=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["dbname"]
            )
        return cls._instance

    def get_connection(self):
        return self._connection_pool.getconn()

    def release_connection(self, connection):
        if connection:
            self._connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls):
        if cls._connection_pool:
            cls._connection_pool.closeall()

    def __del__(self):
        self.close_all_connections()

    def execute_query(self, query, params=None, fetch_one=False, fetch_all=False, commit=False):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        result = None
        if fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        if commit:
            conn.commit()
        cursor.close()
        self.release_connection(conn)
        return result