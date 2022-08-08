from dataclasses import dataclass
from time import time
from typing import List

import pandas as pd
from alpaca_trade_api.rest import TimeFrame
from pandas import DataFrame
from sqlalchemy.engine import Engine

from repository._broker import get_engine, load_query
from collector.alpaca import APIClientAlpaca
from processor.alpaca import price_fluctuation


@dataclass
class RepositoryAlpaca:
    tbl_hist_bar: str = 'alpaca_historical_bar'
    tbl_price_fluct: str = 'alpaca_price_fluctuation'
    engine: Engine = get_engine()

    def store_hist_bars(self, symbols: List[str], timeframe: TimeFrame, if_exist: str = 'append') -> None:
        print('idx | symbol | time_dl | time_store')
        apic_alpaca = APIClientAlpaca()
        for i, symbol in enumerate(symbols):
            t_start = time()
            df = apic_alpaca.download_df_hist_bar(symbol, timeframe)
            t_end_get_df = time()
            df.to_sql(self.tbl_hist_bar, con=self.engine, if_exists=if_exist, index=False)
            t_end_store_db = time()
            print(f'{i}\t{symbol}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')

    def fetch_hist_bar(self, symbol: str, timeframe: TimeFrame) -> DataFrame:
        query = load_query('alpaca', 'select', 'symbol')
        df = pd.read_sql(query, con=self.engine, params=(symbol, timeframe.value))
        return df

    def store_price_fluct(self, symbol: str, timeframe: TimeFrame, if_exist: str = 'append') -> None:
        df = self.fetch_hist_bar(symbol, timeframe)
        df_fluct = price_fluctuation(df)
        pass


def main() -> None:
    rp_alpaca = RepositoryAlpaca()
    df = rp_alpaca.fetch_hist_bar('AAPL', TimeFrame.Day)
    df_c = price_fluctuation(df)
    df_c.to_csv('AAPL_1Day.csv')


if __name__ == '__main__':
    main()
