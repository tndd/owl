from repository import RepositoryHistoricalBarAlp
from repository.resource import BrokerDB
from tests.mock_data.broker import BrokerMockData, DataGroup


def get_test_repository() -> RepositoryHistoricalBarAlp:
    bkr_db = BrokerDB(database='__test_sage_owl')
    return RepositoryHistoricalBarAlp(bkr_db=bkr_db)


def test_store_hist_bar():
    rp = get_test_repository()
    df = BrokerMockData().load_data(DataGroup.HIST_BAR, 'AAPL_1Day')
    print(df)
    # rp.store_hist_bar(df)


def main() -> None:
    test_store_hist_bar()


if __name__ == '__main__':
    main()
