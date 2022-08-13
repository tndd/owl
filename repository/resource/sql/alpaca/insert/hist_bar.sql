REPLACE INTO sage_owl.alpaca_historical_bar(
    symbol,
    timeframe,
    `timestamp`,
    `open`,
    high,
    low,
    `close`,
    volume,
    trade_count,
    vwap
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
