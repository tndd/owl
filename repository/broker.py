from glob import glob
from pathlib import Path
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

pwd: str = Path(__file__).resolve().parent


def get_engine() -> Engine:
    connection_config = {
        'user': 'root',
        'password': 'password',
        'host': 'localhost',
        'port': 3306,
        'database': 'sage_owl'
    }
    return create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config))


def load_tickers() -> List[str]:
    tickers = []
    paths = glob(f'{pwd}/tickers/*.txt')
    for path in paths:
        with open(path, 'r') as f:
            tickers.extend([t.rstrip('\n') for t in f.readlines()])
    return tickers


def load_query(group: str, command: str, name: str) -> str:
    path = f'{pwd}/sql/{group}/{command}/{name}.sql'
    with open(path, 'r') as f:
        query = f.read()
    return query
