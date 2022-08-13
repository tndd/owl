from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class QueryGroup(Enum):
    ALPACA = 'alpaca'


class QueryCommand(Enum):
    CREATE = 'create'
    INSERT = 'insert'
    SELECT = 'select'


@dataclass
class BrokerQuery:
    pwd: str = Path(__file__).resolve().parent

    def load_query(self, group: QueryGroup, command: QueryCommand, name: str) -> str:
        path = f'{self.pwd}/sql/{group.value}/{command.value}/{name}.sql'
        with open(path, 'r') as f:
            query = f.read()
        return query


def main() -> None:
    bq = BrokerQuery()
    q = bq.load_query(QueryGroup.ALPACA, QueryCommand.SELECT, 'hist_bar')
    print(q)


if __name__ == '__main__':
    main()
