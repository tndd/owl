import re

import pytest

from repository import RepositoryHistoricalBarAlp
from repository.db import BrokerDB
from repository.type import Timeframe
from tests.mock_data.broker import BrokerMockData, DataGroup

NAME_TEST_DB = '__test_sage_owl'
NAME_TBL = 'alpaca_historical_bar'


@pytest.fixture
def rp_test() -> RepositoryHistoricalBarAlp:
    bkr_db = BrokerDB(database=NAME_TEST_DB)
    return RepositoryHistoricalBarAlp(bkr_db=bkr_db)


def check_is_test_db(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    pattern = r'__test*'
    db_name = rp_hist_bar.bkr_db.database
    if not re.match(pattern, db_name):
        raise Exception(f'This method can only be called on "{pattern}" database.')


def drop_table_hist_bar(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    check_is_test_db(rp_hist_bar)
    query = f'drop table if exists {NAME_TBL};'
    rp_hist_bar.bkr_db.execute(query)


def truncate_table_hist_bar(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    check_is_test_db(rp_hist_bar)
    query = f'truncate table {NAME_TBL};'
    rp_hist_bar.bkr_db.execute(query)


def test_prepare(rp_test) -> None:
    # clean table
    drop_table_hist_bar(rp_test)
    # create table
    rp_test.prepare()
    # test create_tbl_hist_bar
    expd_scheme = [
        ('symbol', b'varchar(32)', 'NO', 'PRI', None, ''),
        ('timeframe', b'varchar(32)', 'NO', 'PRI', None, ''),
        ('timestamp', b'datetime', 'NO', 'PRI', None, ''),
        ('open', b'decimal(16,8)', 'NO', '', None, ''),
        ('high', b'decimal(16,8)', 'NO', '', None, ''),
        ('low', b'decimal(16,8)', 'NO', '', None, ''),
        ('close', b'decimal(16,8)', 'NO', '', None, ''),
        ('volume', b'int unsigned', 'NO', '', None, ''),
        ('trade_count', b'int unsigned', 'NO', '', None, ''),
        ('vwap', b'decimal(16,8)', 'NO', '', None, '')
    ]
    rp_test.bkr_db.cur.execute(f'DESC {rp_test.tbl_name};')
    assert expd_scheme == rp_test.bkr_db.cur.fetchall()


def test_store_fetch(rp_test) -> None:
    # clean table
    truncate_table_hist_bar(rp_test)
    # store mock data
    mock_df = BrokerMockData().load_mock_df(DataGroup.HIST_BAR_ALP, 'AAPL_1Day')
    rp_test.store(mock_df)
    # fetch mock data
    fetch_df = rp_test.fetch('AAPL', Timeframe.DAY)
    # validate mock & fetched data
    assert mock_df.to_csv() == fetch_df.to_csv()


def main() -> None:
    pass


if __name__ == '__main__':
    main()
