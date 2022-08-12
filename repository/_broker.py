import yaml
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


def load_symbols(group: str = 'ALL') -> List[str]:
    with open(f'{pwd}/symbols.yml') as f:
        symbols = yaml.safe_load(f)
    if group == 'ALL':
        return sum(list(symbols.values()), [])
    else:
        return symbols[group]


def load_query(group: str, command: str, name: str) -> str:
    path = f'{pwd}/sql/{group}/{command}/{name}.sql'
    with open(path, 'r') as f:
        query = f.read()
    return query


def main() -> None:
    print(load_symbols())


if __name__ == '__main__':
    main()
