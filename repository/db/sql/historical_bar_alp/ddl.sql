CREATE TABLE IF NOT EXISTS `alpaca_historical_bar` (
  `symbol` varchar(32) NOT NULL,
  `timeframe` varchar(32) NOT NULL,
  `timestamp` datetime NOT NULL,
  `open` decimal(16,8) NOT NULL,
  `high` decimal(16,8) NOT NULL,
  `low` decimal(16,8) NOT NULL,
  `close` decimal(16,8) NOT NULL,
  `volume` int unsigned NOT NULL,
  `trade_count` int unsigned NOT NULL,
  `vwap` decimal(16,8) NOT NULL,
  PRIMARY KEY (`symbol`,`timeframe`,`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
