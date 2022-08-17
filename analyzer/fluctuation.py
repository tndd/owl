from tests.mock_data.broker import BrokerMockData, DataGroup


def main() -> None:
    df = BrokerMockData().load_data(DataGroup.FLUCT, 'AAPL_1Hour')
    x_train = df.drop(['symbol', 'timeframe', 'ts', 'o0', 'h0', 'l0', 'c0', 'v0'], axis=1)
    y_train = df['o0']
    print(x_train)
    print(y_train)


if __name__ == '__main__':
    main()
