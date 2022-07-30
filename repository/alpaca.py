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
        # preparation
        back_size += 1
        # make index
        index = []
        for col in ['o', 'h', 'l', 'c', 'v']:
            for n in range(back_size)[::-1]:
                index.append(f'{col}{n}')
        sr_vlts = []
        # process df
        for i, r in self.df[span_long:].iterrows():
            # calc volatilities
            df_prev = self.df[['open', 'high', 'low', 'close', 'volume']][i-back_size:i].reset_index(drop=True)
            df_now = self.df[['open', 'high', 'low', 'close', 'volume']][i-back_size+1:i+1].reset_index(drop=True)
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
            avg_short = self.df[i-span_short:i]['close'].mean()
            avg_mid = self.df[i-span_mid:i]['close'].mean()
            avg_long = self.df[i-span_long:i]['close'].mean()
            avg_volume = self.df[i-span_long:i]['volume'].mean()
            sr_vlt['avg_s'] = ((self.df['close'][i-1] - avg_short) / avg_short) * 10000
            sr_vlt['avg_m'] = ((self.df['close'][i-1] - avg_mid) / avg_mid) * 10000
            sr_vlt['avg_l'] = ((self.df['close'][i-1] - avg_long) / avg_long) * 10000
            sr_vlt['avg_v'] = ((self.df['volume'][i-1] - avg_volume) / avg_volume) * 100
            # store series
            sr_vlts.append(sr_vlt)
            print(sr_vlt)
        # make df_price_fluct
        df_price_fluct = pd.concat(sr_vlts, axis=1).T.set_index('ts', drop=True)
        df_price_fluct['symbol'] = self.df['symbol'][0]
        df_price_fluct['time_scale'] = self.df['time_scale'][0]
        return df_price_fluct


def main() -> None:
    rp_alpaca = RepositoryAlpaca()
    df = rp_alpaca.load_df('AAPL', TimeFrame.Day)
    pr_alpaca = ProcessorAlpaca(df)
    df_c = pr_alpaca.price_fluctuation()
    df_c.to_csv('df_concat.csv')


if __name__ == '__main__':
    main()
