SELECT
    symbol,
    time_scale,
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
    time_scale = %s
;
