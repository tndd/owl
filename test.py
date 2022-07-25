from dotenv import load_dotenv
from alpaca_trade_api.rest import REST, TimeFrame
import psycopg2
from sqlalchemy import create_engine

load_dotenv()
api = REST()

connection_config = {
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': 5432,
    'database': 'sage_owl'
}
connection = psycopg2.connect(**connection_config)
engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config))


with open('tickers_dow30.txt', 'r') as f:
    tickers = [t.rstrip('\n') for t in f.readlines()]


for ticker in tickers:
    df = api.get_bars(ticker, TimeFrame.Minute, "2022-07-22", "2022-07-24", adjustment='raw').df
    df.insert(0, 'time_scale', TimeFrame.Minute.value)
    df.insert(0, 'symbol', ticker)
    df.to_sql('stock_bar', con=engine, if_exists='append')
    print(f'Complete: {ticker}')
    print(df)
    exit()

