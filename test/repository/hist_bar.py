from test.data import BrokerData, DataGroup

from repository import RepositoryHistoricalBar
from repository.resource import BrokerDB


def get_test_repository() -> RepositoryHistoricalBar:
    bkr_db = BrokerDB(database='test_sage_owl')
    return RepositoryHistoricalBar(bkr_db=bkr_db)


def test_store_hist_bar():
    rp = get_test_repository()
    df = BrokerData().load_data(DataGroup.HIST_BAR, 'AAPL_1Day')
    rp.store_hist_bar(df)


def main() -> None:
    test_store_hist_bar()


if __name__ == '__main__':
    main()
