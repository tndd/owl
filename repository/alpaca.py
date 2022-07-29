from dotenv import load_dotenv
from alpaca_trade_api.rest import REST, TimeFrame
from sqlalchemy import create_engine
from time import time
from glob import glob
from pathlib import Path


load_dotenv()


def get_engine():
    connection_config = {
        'user': 'root',
        'password': 'password',
        'host': 'localhost',
        'port': 3306,
        'database': 'sage_owl'
    }
    return create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config))

def load_tickers():
    tickers = []
    f_cd = Path(__file__).resolve().parent
    paths = glob(f'{f_cd}/tickers/*.txt')
    for path in paths:
        with open(path, 'r') as f:
            tickers.extend([t.rstrip('\n') for t in f.readlines()])
    return tickers

def download_df_ticker(api: REST, ticker: str, time_frame: TimeFrame):
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


api = REST()
engine = get_engine()
tickers = load_tickers()


print('idx | ticker | time_dl | time_store')
for i, ticker in enumerate(tickers):
    t_start = time()
    df = download_df_ticker(api, ticker, TimeFrame.Day)
    t_end_get_df = time()
    df.to_sql('alpaca_hist_bar', con=engine, if_exists='append', index=False)
    t_end_store_db = time()
    print(f'{i}\t{ticker}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')

