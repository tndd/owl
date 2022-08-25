from dataclasses import dataclass

from pandas import DataFrame, read_sql

from repository.db import (
    BrokerDB,
    BrokerQuery,
    QueryGroup
)
from repository.type import Timeframe


@dataclass
class RepositoryHistoricalBarAlp:
    bkr_db: BrokerDB = BrokerDB()
    bkr_query: BrokerQuery = BrokerQuery(QueryGroup.HISTORICAL_BAR_ALP)

    def prepare(self) -> None:
        query = self.bkr_query.load_query('ddl')
        self.bkr_db.execute(query)

    def store(self, df_hist_bar: DataFrame) -> None:
        # convert type
        df_hist_bar['timestamp'] = df_hist_bar['timestamp'].astype(str)
        # make args
        query = self.bkr_query.load_query('insert')
        params = df_hist_bar.to_records(index=False).tolist()
        # execute
        self.bkr_db.execute_many(query, params) # noqa

    def fetch(self, symbol: str, timeframe: Timeframe) -> DataFrame:
        query = self.bkr_query.load_query('select')
        df = read_sql(query, con=self.bkr_db.engine, params=(symbol, timeframe.value))
        return df

    def fetch_latest_date(self, symbol: str, timeframe: Timeframe) -> str:
        query = self.bkr_query.load_query('select_latest_date')
        df = read_sql(query, con=self.bkr_db.engine, params=(symbol, timeframe.value))
        return df['timestamp'][0].strftime('%Y-%m-%dT%H:%M:%SZ')


def main() -> None:
    pass


if __name__ == '__main__':
    main()
