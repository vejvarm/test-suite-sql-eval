import psycopg2
import asyncio
import os

class PostgresConnector:
    def __init__(self, host="localhost", port=5432, user="sm3", password="secretserver", dbname="sm3"):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }

    def connect(self):
        return psycopg2.connect(**self.conn_params)

    async def query_postgresql(self, query: str):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return "result", result
        except Exception as e:
            raise e