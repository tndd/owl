SELECT
    `timestamp`
FROM alpaca_historical_bar
WHERE
    symbol = %s AND
    timeframe = %s
ORDER BY `timestamp` DESC
LIMIT 1
;
