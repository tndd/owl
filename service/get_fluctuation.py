from alpaca_trade_api.rest import TimeFrame
from pandas import DataFrame

from processor import ProcessorHistoricalBar
from repository import RepositoryHistoricalBarAlp


def get_fluctuation(symbol: str = 'AAPL', timeframe: TimeFrame = TimeFrame.Day) -> DataFrame:
    rp_hist_bar = RepositoryHistoricalBarAlp()
    df_hist_bar = rp_hist_bar.fetch_hist_bar(symbol, timeframe)
    df_fluct = ProcessorHistoricalBar(df_hist_bar).fluctuation()
    df_fluct.to_csv(f'{symbol}_{timeframe.value}.csv')
    return df_fluct


def main():
    get_fluctuation()


if __name__ == '__main__':
    main()
