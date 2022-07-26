from dotenv import load_dotenv
from alpaca_trade_api.rest import REST, TimeFrame
from sqlalchemy import create_engine
from time import time
from glob import glob
from pathlib import Path


load_dotenv()


def get_engine():
    connection_config = {
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': 5432,
        'database': 'sage_owl'
    }
    return create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config))

def load_tickers():
    tickers = []
    f_cd = Path(__file__).resolve().parent
    paths = glob(f'{f_cd}/tickers/*.txt')
    for path in paths:
        with open(path, 'r') as f:
            tickers.extend([t.rstrip('\n') for t in f.readlines()])
    return tickers


api = REST()
engine = get_engine()
tickers = load_tickers()


print('idx | ticker | time_dl | time_store')
for i, ticker in enumerate(tickers):
    t_start = time()
    df = api.get_bars(ticker, TimeFrame.Minute, "2012-07-24", "2022-07-24", adjustment='raw').df
    df.insert(0, 'time_scale', TimeFrame.Minute.value)
    df.insert(0, 'symbol', ticker)
    t_end_get_df = time()
    df.to_sql('stock_bar', con=engine, if_exists='append')
    t_end_store_db = time()
    print(f'{i}\t{ticker}\t{t_end_get_df - t_start}\t{t_end_store_db - t_start}')

