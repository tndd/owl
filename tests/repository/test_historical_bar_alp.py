import re

import pytest

from repository import RepositoryHistoricalBarAlp
from repository.resource import BrokerDB
from repository.type import Timeframe
from tests.mock_data.broker import BrokerMockData, DataGroup

TEST_DB_NAME = '__test_sage_owl'


@pytest.fixture
def test_rp(database: str = TEST_DB_NAME) -> RepositoryHistoricalBarAlp:
    bkr_db = BrokerDB(database=database)
    return RepositoryHistoricalBarAlp(bkr_db=bkr_db)


def check_is_test_db(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    pattern = r'__test*'
    db_name = rp_hist_bar.bkr_db.database
    if not re.match(pattern, db_name):
        raise Exception(f'This method can only be called on "{pattern}" database.')


def drop_table_hist_bar(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    check_is_test_db(rp_hist_bar)
    query = f'drop table if exists {rp_hist_bar.tbl_hist_bar};'
    rp_hist_bar.bkr_db.execute(query)


def truncate_table_hist_bar(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    check_is_test_db(rp_hist_bar)
    query = f'truncate table {rp_hist_bar.tbl_hist_bar};'
    rp_hist_bar.bkr_db.execute(query)


def test_create_tbl_hist_bar(test_rp: RepositoryHistoricalBarAlp) -> None:
    # clean table
    drop_table_hist_bar(test_rp)
    # create table
    test_rp.create_tbl_hist_bar()
    # test create_tbl_hist_bar
    expd_scheme = [
        ('symbol', b'varchar(64)', 'NO', 'PRI', None, ''),
        ('timeframe', b'varchar(64)', 'NO', 'PRI', None, ''),
        ('timestamp', b'datetime', 'NO', 'PRI', None, ''),
        ('open', b'decimal(12,6)', 'NO', '', None, ''),
        ('high', b'decimal(12,6)', 'NO', '', None, ''),
        ('low', b'decimal(12,6)', 'NO', '', None, ''),
        ('close', b'decimal(12,6)', 'NO', '', None, ''),
        ('volume', b'int unsigned', 'NO', '', None, ''),
        ('trade_count', b'int unsigned', 'NO', '', None, ''),
        ('vwap', b'decimal(12,6)', 'NO', '', None, '')
    ]
    test_rp.bkr_db.cur.execute(f'DESC {test_rp.tbl_hist_bar};')
    assert expd_scheme == test_rp.bkr_db.cur.fetchall()


def test_store_fetch(test_rp: RepositoryHistoricalBarAlp) -> None:
    # clean table
    truncate_table_hist_bar(test_rp)
    # store mock data
    mock_df = BrokerMockData().load_mock_df(DataGroup.HIST_BAR_ALP, 'AAPL_1Day')
    test_rp.store_hist_bar(mock_df)
    # fetch mock data
    fetch_df = test_rp.fetch_hist_bar('AAPL', Timeframe.DAY)
    # validate mock & fetched data
    assert mock_df.to_csv() == fetch_df.to_csv()


def main() -> None:
    pass


if __name__ == '__main__':
    main()
