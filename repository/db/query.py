from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class QueryGroup(Enum):
    HISTORICAL_BAR_ALP = 'historical_bar_alp'
    HISTORICAL_BAR_FMP = 'historical_bar_fmp'



@dataclass
class BrokerQuery:
    group: QueryGroup
    pwd: str = Path(__file__).resolve().parent

    def load_query(self, name: str) -> str:
        path = f'{self.pwd}/sql/{self.group.value}/{name}.sql'
        with open(path, 'r') as f:
            query = f.read()
        return query


def main() -> None:
    bq = BrokerQuery(QueryGroup.HISTORICAL_BAR_ALP)
    q = bq.load_query('select')
    print(q)


if __name__ == '__main__':
    main()
