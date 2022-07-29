from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class Broker:
    # Database
    user: str = 'root'
    password: str = 'password'
    host: str = 'localhost'
    port: int = 3306
    database: str = 'sage_owl'
    # SQL
    pwd: str = Path(__file__).resolve().parent

    def load_tickers_from_file(self) -> List[str]:
        tickers = []
        paths = glob(f'{self.pwd}/tickers/*.txt')
        for path in paths:
            with open(path, 'r') as f:
                tickers.extend([t.rstrip('\n') for t in f.readlines()])
        return tickers

    def load_query(self, group: str, command: str, name: str) -> str:
        path = f'{self.pwd}/sql/{group}/{command}/{name}.sql'
        with open(path, 'r') as f:
            query = f.read()
        return query

    def get_engine(self) -> Engine:
        return create_engine(
            f'mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )
