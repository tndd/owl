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
    tbl_hist_bar: str = 'alpaca_historical_bar'
    table_name: str = 'alpaca_hist_bar'
    api: REST = REST()
    engine: Engine = get_engine()

    def download_df_ticker(self, ticker: str, timeframe: TimeFrame, adjustment: str = 'all') -> DataFrame:
        df = self.api.get_bars(
            ticker,
            timeframe,
            self.date_range_start,
            self.date_range_end,
            adjustment=adjustment
        ).df.reset_index()
        df.insert(0, 'symbol', ticker)
        df.insert(1, 'timeframe', timeframe.value)
        return df

    def store_tickers(self, tickers: List[str], timeframe: TimeFrame, if_exist: str = 'append') -> None:
        print('idx | ticker | time_dl | time_store')
        for i, ticker in enumerate(tickers):
            t_start = time()
            df = self.download_df_ticker(ticker, timeframe)
            t_end_get_df = time()
            df.to_sql(self.tbl_hist_bar, con=self.engine, if_exists=if_exist, index=False)
            t_end_store_db = time()
            print(f'{i}\t{ticker}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')

    def fetch_ticker(self, ticker: str, timeframe: TimeFrame) -> DataFrame:
        query = load_query('alpaca', 'select', 'symbol')
        df = pd.read_sql(query, con=self.engine, params=(ticker, timeframe.value))
        return df

    def store_fluctuation(self, ticker: str, timeframe: TimeFrame) -> None:
        pass


def price_fluctuation(
    df: DataFrame,
    span_short: int = 6,
    span_mid: int = 36,
    span_long: int = 216,
    back_size: int = 10
) -> DataFrame:
    # preparation
    back_size += 1
    # make index
    index = []
    for col in ['o', 'h', 'l', 'c', 'v']:
        for n in range(back_size)[::-1]:
            index.append(f'{col}{n}')
    sr_vlts = []
    # process df
    for i, r in df[span_long:].iterrows():
        # calc volatilities
        df_prev = df[['open', 'high', 'low', 'close', 'volume']][i-back_size:i].reset_index(drop=True)
        df_now = df[['open', 'high', 'low', 'close', 'volume']][i-back_size+1:i+1].reset_index(drop=True)
        df_vlt = (df_now - df_prev) / df_prev
        # basis point
        df_vlt[['open', 'high', 'low', 'close']] = df_vlt[['open', 'high', 'low', 'close']] * 10000
        # percent
        df_vlt['volume'] = df_vlt['volume'] * 100
        # make one line
        sr_vlt = df_vlt.melt()
        sr_vlt['col'] = index
        sr_vlt = sr_vlt.set_index('col')['value']
        sr_vlt['ts'] = r['timestamp']
        # calc average
        avg_short = df[i-span_short:i]['close'].mean()
        avg_mid = df[i-span_mid:i]['close'].mean()
        avg_long = df[i-span_long:i]['close'].mean()
        avg_volume = df[i-span_long:i]['volume'].mean()
        sr_vlt['avg_s'] = ((df['close'][i-1] - avg_short) / avg_short) * 10000
        sr_vlt['avg_m'] = ((df['close'][i-1] - avg_mid) / avg_mid) * 10000
        sr_vlt['avg_l'] = ((df['close'][i-1] - avg_long) / avg_long) * 10000
        sr_vlt['avg_v'] = ((df['volume'][i-1] - avg_volume) / avg_volume) * 100
        # store series
        sr_vlts.append(sr_vlt)
        print(sr_vlt)
    # make df_price_fluct
    df_price_fluct = pd.concat(sr_vlts, axis=1).T.set_index('ts', drop=True)
    df_price_fluct['symbol'] = df['symbol'][0]
    df_price_fluct['timeframe'] = df['timeframe'][0]
    return df_price_fluct


def main() -> None:
    rp_alpaca = RepositoryAlpaca()
    df = rp_alpaca.fetch_ticker('AAPL', TimeFrame.Day)
    df_c = price_fluctuation(df)
    df_c.to_csv('df_concat.csv')


if __name__ == '__main__':
    main()
