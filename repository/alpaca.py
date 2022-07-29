from time import time
from typing import List

import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy.engine import Engine

from repository.broker import get_engine, load_query, load_tickers

load_dotenv()


def download_df_ticker(api: REST, ticker: str, time_frame: TimeFrame) -> DataFrame:
    default_time_start = "2012-07-24"
    default_time_end = "2022-07-24"
    df = api.get_bars(
        ticker,
        time_frame,
        default_time_start,
        default_time_end,
        adjustment='all'
    ).df.reset_index()
    df.insert(0, 'symbol', ticker)
    df.insert(1, 'time_scale', time_frame.value)
    return df


def store_tickers_to_db(api: REST, engine: Engine, tickers: List[str], time_frame: TimeFrame) -> None:
    table_name = 'alpaca_hist_bar'
    print('idx | ticker | time_dl | time_store')
    for i, ticker in enumerate(tickers):
        t_start = time()
        df = download_df_ticker(api, ticker, time_frame)
        t_end_get_df = time()
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        t_end_store_db = time()
        print(f'{i}\t{ticker}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')


def load_df(ticker: str, time_frame: TimeFrame) -> DataFrame:
    query = load_query('alpaca', 'select', 'symbol')
    print(query)


def main() -> None:
    api = REST()
    engine = get_engine()
    tickers = load_tickers()
    store_tickers_to_db(api, engine, tickers, TimeFrame.Day)


if __name__ == '__main__':
    load_df('AAPL', TimeFrame.Day)

