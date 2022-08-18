from dataclasses import dataclass

from pandas import DataFrame, read_sql

from repository.resource import (
    BrokerDB,
    BrokerQuery,
    QueryCommand,
    QueryGroup
)
from repository.type import Timeframe


@dataclass
class RepositoryHistoricalBarAlp:
    tbl_hist_bar: str = 'alpaca_historical_bar'
    bkr_db: BrokerDB = BrokerDB()
    bkr_query: BrokerQuery = BrokerQuery()

    def create_tbl_hist_bar(self) -> None:
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.CREATE, 'hist_bar')
        self.bkr_db.execute(query)

    def store_hist_bar(self, df_hist_bar: DataFrame) -> None:
        # convert type
        df_hist_bar['timestamp'] = df_hist_bar['timestamp'].astype(str)
        # make args
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.INSERT, 'hist_bar')
        params = df_hist_bar.to_records(index=False).tolist()
        # execute
        self.bkr_db.execute_many(query, params) # noqa

    def fetch_hist_bar(self, symbol: str, timeframe: Timeframe) -> DataFrame:
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.SELECT, 'hist_bar')
        df = read_sql(query, con=self.bkr_db.engine, params=(symbol, timeframe.value))
        return df

    def fetch_latest_date(self, symbol: str, timeframe: Timeframe) -> str:
        query = self.bkr_query.load_query(QueryGroup.ALPACA, QueryCommand.SELECT, 'hist_bar_latest_date')
        df = read_sql(query, con=self.bkr_db.engine, params=(symbol, timeframe.value))
        return df['timestamp'][0].strftime('%Y-%m-%dT%H:%M:%SZ')


def main() -> None:
    pass


if __name__ == '__main__':
    main()
