from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pandas as pd
from pandas import DataFrame


class DataGroup(Enum):
    HIST_BAR = 'historical_bar_alp'
    FLUCT = 'fluctuation'


@dataclass
class BrokerMockData:
    pwd: str = Path(__file__).resolve().parent

    def load_data(self, group: DataGroup, name: str) -> DataFrame:
        path = f'{self.pwd}/{group.value}/{name}.csv'
        return pd.read_csv(path)


def main() -> None:
    bd = BrokerMockData()
    df = bd.load_data(DataGroup.HIST_BAR, 'AAPL_1Day')
    print(df)


if __name__ == '__main__':
    main()
