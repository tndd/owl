import pandas as pd


def main() -> None:
    df = pd.read_csv('AAPL_1Day.csv')
    x_train = df.drop(['symbol', 'timeframe', 'ts', 'o0', 'h0', 'l0', 'c0', 'v0'], axis=1)
    y_train = df['o0']
    print(x_train)
    print(y_train)


if __name__ == '__main__':
    main()
