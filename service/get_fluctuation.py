from alpaca_trade_api.rest import TimeFrame
from pandas import DataFrame

from repository import RepositoryHistoricalBar
from processor import ProcessorHistoricalBar


def get_fluctuation(symbol: str = 'AAPL', timeframe: TimeFrame = TimeFrame.Day) -> DataFrame:
    rp_hist_bar = RepositoryHistoricalBar()
    df_hist_bar = rp_hist_bar.fetch_hist_bar(symbol, timeframe)
    df_fluct = ProcessorHistoricalBar(df_hist_bar).fluctuation()
    df_fluct.to_csv(f'{symbol}_{timeframe.value}.csv')
    return df_fluct


def main():
    get_fluctuation()


if __name__ == '__main__':
    main()
