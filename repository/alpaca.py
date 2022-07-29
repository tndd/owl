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
        span_short: int = 5,
        span_mid: int = 25,
        span_long: int = 125,
        size: int = 10
    ) -> DataFrame:
        df_0 = self.df[['open', 'high', 'low', 'close', 'volume']][:10].reset_index(drop=True)
        df_1 = self.df[['open', 'high', 'low', 'close', 'volume']][1:11].reset_index(drop=True)
        print(df_0.head(3))
        print('---------------')
        print(df_1.head(3))
        print('---------------')
        df_prd = (df_1 - df_0) / df_0
        df_prd[['open', 'high', 'low', 'close']] = df_prd[['open', 'high', 'low', 'close']] * 10000
        df_prd['volume'] = df_prd['volume'] * 100
        print(df_prd)
        return
        # for index, row in self.df[span_long:].iterrows():
        #     avg_short = self.df[index-span_short:index]['close'].mean()
        #     avg_mid = self.df[index-span_mid:index]['close'].mean()
        #     avg_long_cv = self.df[index-span_long:index][['close', 'volume']].mean()
        #     return


def main() -> None:
    rp_alpaca = RepositoryAlpaca()
    df = rp_alpaca.load_df('AAPL', TimeFrame.Day)
    pr_alpaca = ProcessorAlpaca(df)
    print(pr_alpaca.price_fluctuation())


if __name__ == '__main__':
    main()

