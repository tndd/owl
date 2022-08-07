import pandas as pd
from pandas import DataFrame


def price_fluctuation(
    df: DataFrame,
    span_short: int = 6,
    span_mid: int = 36,
    span_long: int = 216,
    back_size: int = 10
) -> DataFrame:
    # preparation
    back_size += 1
    # make index
    index = []
    for col in ['o', 'h', 'l', 'c', 'v']:
        for n in range(back_size)[::-1]:
            index.append(f'{col}{n}')
    sr_vlts = []
    # calc intraday rate
    df[['v_high', 'v_low', 'v_close']] = ((df[['high', 'low', 'close']].T - df['open']) / df['open']).T * 10000
    # process df
    for i, r in df[span_long:].iterrows():
        # calc volatilities
        df_prev = df[['open', 'volume']][i-back_size:i].reset_index(drop=True)
        df_now = df[['open', 'v_high', 'v_low', 'v_close', 'volume']][i-back_size+1:i+1].reset_index(drop=True)
        df_now[['open', 'volume']] = (df_now[['open', 'volume']] - df_prev) / df_prev
        # calc rate as basis point
        df_now['open'] = df_now['open'] * 10000
        # calc rate as percent
        df_now['volume'] = df_now['volume'] * 100
        # make one line
        sr_vlt = df_now.melt()
        sr_vlt['col'] = index
        sr_vlt = sr_vlt.set_index('col')['value']
        sr_vlt['ts'] = r['timestamp']
        # calc average
        avg_short = df[i-span_short:i]['close'].mean()
        avg_mid = df[i-span_mid:i]['close'].mean()
        avg_long = df[i-span_long:i]['close'].mean()
        avg_volume = df[i-span_long:i]['volume'].mean()
        sr_vlt['avg_s'] = ((df['close'][i-1] - avg_short) / avg_short) * 10000
        sr_vlt['avg_m'] = ((df['close'][i-1] - avg_mid) / avg_mid) * 10000
        sr_vlt['avg_l'] = ((df['close'][i-1] - avg_long) / avg_long) * 10000
        sr_vlt['avg_v'] = ((df['volume'][i-1] - avg_volume) / avg_volume) * 100
        # store series
        sr_vlts.append(sr_vlt)
        print(sr_vlt)
    # make df_price_fluct
    df_price_fluct = pd.concat(sr_vlts, axis=1).T.set_index('ts', drop=True)
    df_price_fluct['symbol'] = df['symbol'][0]
    df_price_fluct['timeframe'] = df['timeframe'][0]
    return df_price_fluct
