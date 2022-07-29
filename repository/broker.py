from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class Broker:
    user: str = 'root'
    password: str = 'password'
    host: str = 'localhost'
    port: int = 3306
    database: str = 'sage_owl'

    @staticmethod
    def load_tickers_from_file() -> List[str]:
        tickers = []
        f_cd = Path(__file__).resolve().parent
        paths = glob(f'{f_cd}/tickers/*.txt')
        for path in paths:
            with open(path, 'r') as f:
                tickers.extend([t.rstrip('\n') for t in f.readlines()])
        return tickers

    def get_engine(self) -> Engine:
        return create_engine(
            f'mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database})'
        )
