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

    def load_df(self, ticker: str, time_frame: TimeFrame) -> DataFrame:
        query = load_query('alpaca', 'select', 'symbol')
        df = pd.read_sql(query, con=self.engine, params=(ticker, time_frame.value))
        return df


@dataclass
class ProcessorAlpaca:
    df: DataFrame

    def price_fluctuation(
        self,
        span_short: int = 6,
        span_mid: int = 36,
        span_long: int = 216,
        back_size: int = 10
    ) -> DataFrame:
        back_size += 1
        # make index
        index = []
        for col in ['o', 'h', 'l', 'c', 'v']:
            for n in range(back_size):
                index.append(f'{col}{n}')
        index.insert(0, 'ts')
        index.extend(['avg_s', 'avg_m', 'avg_l', 'avg_v'])
        # process df
        for i, r in self.df[span_long:].iterrows():
            # calc average volatility
            avg_short = self.df[i-span_short:i]['close'].mean()
            avg_mid = self.df[i-span_mid:i]['close'].mean()
            avg_long_cv = self.df[i-span_long:i][['close', 'volume']].mean()
            # calc volatilities
            df_prev = self.df[['open', 'high', 'low', 'close', 'volume']][i-back_size:i].reset_index(drop=True)
            df_now = self.df[['open', 'high', 'low', 'close', 'volume']][i-back_size+1:i+1].reset_index(drop=True)
            df_vlt = (df_now - df_prev) / df_prev
            # basis point
            df_vlt[['open', 'high', 'low', 'close']] = df_vlt[['open', 'high', 'low', 'close']] * 10000
            # percent
            df_vlt['volume'] = df_vlt['volume'] * 100
            print(df_vlt)
            return


def main() -> None:
    rp_alpaca = RepositoryAlpaca()
    df = rp_alpaca.load_df('AAPL', TimeFrame.Day)
    pr_alpaca = ProcessorAlpaca(df)
    print(pr_alpaca.price_fluctuation())


if __name__ == '__main__':
    main()

