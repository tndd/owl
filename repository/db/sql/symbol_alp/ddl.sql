CREATE TABLE `alpaca_assets` (
  `time_group` datetime NOT NULL,
  `id` varchar(36) NOT NULL,
  `class` varchar(32) NOT NULL,
  `exchange` varchar(32) NOT NULL,
  `symbol` varchar(32) NOT NULL,
  `name` varchar(256) NOT NULL,
  `status` varchar(16) NOT NULL,
  `tradable` tinyint(1) NOT NULL,
  `marginable` tinyint(1) NOT NULL,
  `maintenance_margin_requirement` INT UNSIGNED NOT NULL,
  `shortable` tinyint(1) NOT NULL,
  `easy_to_borrow` tinyint(1) NOT NULL,
  `fractionable` tinyint(1) NOT NULL,
  `min_order_size` decimal(16,8) DEFAULT NULL,
  `min_trade_increment` decimal(16,8) DEFAULT NULL,
  `price_increment` decimal(16,8) DEFAULT NULL,
  PRIMARY KEY (`time_group`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
