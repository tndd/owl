CREATE TABLE `alpaca_historical_bar` (
  `symbol` varchar(64) NOT NULL,
  `timeframe` varchar(64) NOT NULL,
  `timestamp` datetime NOT NULL,
  `open` decimal(12,6) NOT NULL,
  `high` decimal(12,6) NOT NULL,
  `low` decimal(12,6) NOT NULL,
  `close` decimal(12,6) NOT NULL,
  `volume` int unsigned NOT NULL,
  `trade_count` int unsigned NOT NULL,
  `vwap` decimal(12,6) NOT NULL,
  PRIMARY KEY (`symbol`,`timeframe`,`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
