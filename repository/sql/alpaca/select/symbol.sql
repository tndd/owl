SELECT
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
FROM sage_owl.alpaca_hist_bar
WHERE
    symbol = %s and
    timeframe = %s
;
