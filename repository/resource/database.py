from dataclasses import dataclass
from typing import List

from mysql.connector import MySQLConnection, connect
from mysql.connector.cursor import MySQLCursor
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class BrokerDB:
    user: str = 'root'
    password: str = 'password'
    host: str = 'localhost'
    port: str = 3306
    database: str = 'sage_owl'

    def __post_init__(self) -> None:
        self.engine: Engine = self.get_engine_alchemy()
        self.conn: MySQLConnection = self.get_conn_mc()
        self.cur: MySQLCursor = self.conn.cursor()

    def make_db_config(self) -> dict:
        return {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port,
            'database': self.database
        }

    def get_engine_alchemy(self) -> Engine:
        return create_engine(
            'mysql://{user}:{password}@{host}:{port}/{database}'.format(
                **self.make_db_config()
            )
        )

    def get_conn_mc(self) -> MySQLConnection:
        return connect(**self.make_db_config())

    def execute_many(self, query: str, params: List[tuple]) -> None:
        self.cur.executemany(query, params)
        self.conn.commit()
