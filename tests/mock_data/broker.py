from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pandas as pd
from pandas import DataFrame


class DataGroup(Enum):
    HIST_BAR_ALP = 'historical_bar_alp'
    FLUCT = 'fluctuation'


@dataclass
class BrokerMockData:
    pwd: str = Path(__file__).resolve().parent

    def load_mock_df(self, group: DataGroup, name: str) -> DataFrame:
        path = f'{self.pwd}/{group.value}/{name}.csv'
        return pd.read_csv(path)


def main() -> None:
    bd = BrokerMockData()
    df = bd.load_mock_df(DataGroup.HIST_BAR_ALP, 'AAPL_1Day')
    print(df)


if __name__ == '__main__':
    main()
