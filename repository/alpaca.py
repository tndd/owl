from dataclasses import dataclass
from time import time
from typing import List

import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy.engine import Engine

from repository.broker import get_engine, load_query, load_tickers

load_dotenv()


@dataclass
class RepositoryAlpaca:
    date_range_start: str = '2012-07-24'
    date_range_end: str = '2022-07-24'
    table_name: str = 'alpaca_hist_bar'
    api: REST = REST()
    engine: Engine = get_engine()

    def download_df_ticker(self, ticker: str, time_frame: TimeFrame, adjustment: str = 'all') -> DataFrame:
        df = self.api.get_bars(
            ticker,
            time_frame,
            self.date_range_start,
            self.date_range_end,
            adjustment=adjustment
        ).df.reset_index()
        df.insert(0, 'symbol', ticker)
        df.insert(1, 'time_scale', time_frame.value)
        return df

    def download_tickers_to_db(self, tickers: List[str], time_frame: TimeFrame, if_exist: str = 'append') -> None:
        print('idx | ticker | time_dl | time_store')
        for i, ticker in enumerate(tickers):
            t_start = time()
            df = self.download_df_ticker(ticker, time_frame)
            t_end_get_df = time()
            df.to_sql(self.table_name, con=self.engine, if_exists=if_exist, index=False)
            t_end_store_db = time()
            print(f'{i}\t{ticker}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')


def load_df(ticker: str, time_frame: TimeFrame) -> DataFrame:
    query = load_query('alpaca', 'select', 'symbol')
    print(query)


def main() -> None:
    rp_alpaca = RepositoryAlpaca()
    tickers = load_tickers()
    rp_alpaca.download_tickers_to_db(tickers, TimeFrame.Day)


if __name__ == '__main__':
    main()

