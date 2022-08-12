from pathlib import Path

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


def load_query(group: str, command: str, name: str) -> str:
    path = f'{pwd}/sql/{group}/{command}/{name}.sql'
    with open(path, 'r') as f:
        query = f.read()
    return query


def main() -> None:
    pass


if __name__ == '__main__':
    main()
