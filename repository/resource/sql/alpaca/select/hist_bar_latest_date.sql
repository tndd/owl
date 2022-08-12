SELECT
    `timestamp`
FROM sage_owl.alpaca_historical_bar
WHERE
    symbol = %s AND
    timeframe = %s
ORDER BY `timestamp` DESC
LIMIT 1
;
