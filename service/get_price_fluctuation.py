from alpaca_trade_api.rest import TimeFrame
from pandas import DataFrame

from repository.historical_bar import RepositoryHistoricalBar
from processor.historical_bar import ProcessorHistoricalBar


def get_price_fluctuation(symbol: str = 'AAPL', timeframe: TimeFrame = TimeFrame.Day) -> DataFrame:
    rp_hist_bar = RepositoryHistoricalBar()
    df_hist_bar = rp_hist_bar.fetch_hist_bar(symbol, timeframe)
    df_pfluct = ProcessorHistoricalBar(df_hist_bar).price_fluctuation()
    df_pfluct.to_csv(f'{symbol}_{timeframe.value}.csv')
    return df_pfluct


def main():
    get_price_fluctuation()


if __name__ == '__main__':
    main()
