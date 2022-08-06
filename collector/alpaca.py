from dataclasses import dataclass

from alpaca_trade_api.rest import REST, TimeFrame
from dotenv import load_dotenv
from pandas import DataFrame

load_dotenv()


@dataclass
class APIClientAlpaca:
    date_range_start: str = '2012-07-24'
    date_range_end: str = '2022-07-24'
    api: REST = REST()

    def download_df_hist_bar(self, symbol: str, timeframe: TimeFrame, adjustment: str = 'all') -> DataFrame:
        df = self.api.get_bars(
            symbol,
            timeframe,
            self.date_range_start,
            self.date_range_end,
            adjustment=adjustment
        ).df.reset_index()
        df.insert(0, 'symbol', symbol)
        df.insert(1, 'timeframe', timeframe.value)
        return df
