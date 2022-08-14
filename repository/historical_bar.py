from dataclasses import dataclass
from time import time
from typing import List

import pandas as pd
from alpaca_trade_api.rest import TimeFrame
from pandas import DataFrame

from repository.resource import (
    BrokerDB,
    BrokerQuery,
    QueryCommand,
    QueryGroup
)
from collector import APIClientAlpaca


@dataclass
class RepositoryHistoricalBar:
    tbl_hist_bar: str = 'alpaca_historical_bar'
    bkr_db: BrokerDB = BrokerDB()
    bkr_query: BrokerQuery = BrokerQuery()

    def store_hist_bars(self, symbols: List[str], timeframe: TimeFrame, if_exist: str = 'append') -> None:
        print('idx | symbol | time_dl | time_store')
        apic_alpaca = APIClientAlpaca()
        for i, symbol in enumerate(symbols):
            t_start = time()
            df = apic_alpaca.download_hist_bar_df(symbol, timeframe)
            t_end_get_df = time()
            df.to_sql(self.tbl_hist_bar, con=self.bkr_db.engine, if_exists=if_exist, index=False)
            t_end_store_db = time()
            print(f'{i}\t{symbol}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')

    def store_hist_bar(self, df_hist_bar: DataFrame) -> None:
        # convert type
        df_hist_bar['timestamp'] = df_hist_bar['timestamp'].astype(str)
        # make args
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.INSERT, 'hist_bar')
        params = df_hist_bar.to_records(index=False).tolist()
        # execute
        self.bkr_db.execute_many(query, params)

    def fetch_hist_bar(self, symbol: str, timeframe: TimeFrame) -> DataFrame:
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.SELECT, 'hist_bar')
        df = pd.read_sql(query, con=self.bkr_db.engine, params=(symbol, timeframe.value))
        return df

    def fetch_latest_date(self, symbol: str, timeframe: TimeFrame) -> str:
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.SELECT, 'hist_bar_latest_date')
        df = pd.read_sql(query, con=self.bkr_db.engine, params=(symbol, timeframe.value))
        return df['timestamp'][0].strftime('%Y-%m-%dT%H:%M:%SZ')


def main() -> None:
    rp_hist_bar = RepositoryHistoricalBar()
    df = rp_hist_bar.fetch_hist_bar('AAPL', TimeFrame.Day)
    rp_hist_bar.store_hist_bar(df)


if __name__ == '__main__':
    main()
