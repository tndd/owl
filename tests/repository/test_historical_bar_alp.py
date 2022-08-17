import re

from repository import RepositoryHistoricalBarAlp, TimeframeAlpRp
from repository.resource import BrokerDB
from tests.mock_data.broker import BrokerMockData, DataGroup

TEST_DB_NAME = '__test_sage_owl'


def get_repository_for_test(database: str = TEST_DB_NAME) -> RepositoryHistoricalBarAlp:
    bkr_db = BrokerDB(database=database)
    return RepositoryHistoricalBarAlp(bkr_db=bkr_db)


def drop_table_hist_bar(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    pattern = r'__test*'
    db_name = rp_hist_bar.bkr_db.database
    if not re.match(pattern, db_name):
        raise Exception(f'This method can only be called on "{pattern}" database.')
    query = f'drop table if exists {rp_hist_bar.tbl_hist_bar};'
    rp_hist_bar.bkr_db.execute(query)


def truncate_table_hist_bar(rp_hist_bar: RepositoryHistoricalBarAlp) -> None:
    # TODO: decorator
    pattern = r'__test*'
    db_name = rp_hist_bar.bkr_db.database
    if not re.match(pattern, db_name):
        raise Exception(f'This method can only be called on "{pattern}" database.')
    query = f'truncate table {rp_hist_bar.tbl_hist_bar};'
    rp_hist_bar.bkr_db.execute(query)


def test_create_tbl_hist_bar() -> None:
    rp = get_repository_for_test()
    # clean table
    drop_table_hist_bar(rp)
    # create table
    rp.create_tbl_hist_bar()
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
    rp.bkr_db.cur.execute(f'DESC {rp.tbl_hist_bar};')
    assert expd_scheme == rp.bkr_db.cur.fetchall()


def test_store_fetch() -> None:
    rp = get_repository_for_test()
    # clean table
    truncate_table_hist_bar(rp)
    # store mock data
    mock_df = BrokerMockData().load_mock_df(DataGroup.HIST_BAR_ALP, 'AAPL_1Day')
    rp.store_hist_bar(mock_df)
    # fetch mock data
    fetch_df = rp.fetch_hist_bar('AAPL', TimeframeAlpRp.DAY)
    # validate mock & fetched data
    assert mock_df.to_csv() == fetch_df.to_csv()


def main() -> None:
    test_store_fetch()


if __name__ == '__main__':
    main()
