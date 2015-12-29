-- Import TPC-E GenLoader generted Data to TPC-E GemFireXD Schema. Do not need converters.
-- BLOB in NEWS_ITEM is actually the ASCII char string
-- Holding summary is a view of holding now
-- connect client '10.150.30.37:7710;user=tpcegfxd;password=tpcegfxd';
SET SCHEMA TPCEGFXD;
--CALL SQLJ.INSTALL_JAR('/gcm/where/tpcedata/dataconverters.jar', 'SYSCS_UTIL.converter',0);
CALL SQLJ.INSTALL_JAR('/gcm/where/tpcedata/gfxd/dataconverters.jar', 'SYSCS_UTIL.converter',0);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','ZIP_CODE', '/gcm/where/tpcedata/10kdata/flat_out/ZipCode.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','ADDRESS', '/gcm/where/tpcedata/10kdata/flat_out/Address.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','STATUS_TYPE', '/gcm/where/tpcedata/10kdata/flat_out/StatusType.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TAXRATE', '/gcm/where/tpcedata/10kdata/flat_out/Taxrate.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CUSTOMER', '/gcm/where/tpcedata/10kdata/flat_out/Customer.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','EXCHANGE', '/gcm/where/tpcedata/10kdata/flat_out/Exchange.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','SECTOR', '/gcm/where/tpcedata/10kdata/flat_out/Sector.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','INDUSTRY', '/gcm/where/tpcedata/10kdata/flat_out/Industry.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','COMPANY', '/gcm/where/tpcedata/10kdata/flat_out/Company.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','COMPANY_COMPETITOR', '/gcm/where/tpcedata/10kdata/flat_out/CompanyCompetitor.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','SECURITY', '/gcm/where/tpcedata/10kdata/flat_out/Security.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','DAILY_MARKET', '/gcm/where/tpcedata/10kdata/flat_out/DailyMarket.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','FINANCIAL', '/gcm/where/tpcedata/10kdata/flat_out/Financial.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
-- contain real timestamp type of data
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','LAST_TRADE', '/gcm/where/tpcedata/10kdata/flat_out/LastTrade.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','NEWS_ITEM', '/gcm/where/tpcedata/10kdata/flat_out/NewsItem.txt', '|', NULL, NULL, 0, 0, 1, 0, 'ASCIICharBlobImportOra', NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','NEWS_XREF', '/gcm/where/tpcedata/10kdata/flat_out/NewsXRef.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','BROKER', '/gcm/where/tpcedata/10kdata/flat_out/Broker.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CUSTOMER_ACCOUNT', '/gcm/where/tpcedata/10kdata/flat_out/CustomerAccount.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','ACCOUNT_PERMISSION', '/gcm/where/tpcedata/10kdata/flat_out/AccountPermission.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CUSTOMER_TAXRATE', '/gcm/where/tpcedata/10kdata/flat_out/CustomerTaxrate.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TRADE_TYPE', '/gcm/where/tpcedata/10kdata/flat_out/TradeType.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TRADE', '/gcm/where/tpcedata/10kdata/flat_out/Trade.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','SETTLEMENT', '/gcm/where/tpcedata/10kdata/flat_out/Settlement.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TRADE_HISTORY', '/gcm/where/tpcedata/10kdata/flat_out/TradeHistory.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','HOLDING_SUMMARY_TBL', '/gcm/where/tpcedata/10kdata/flat_out/HoldingSummary.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','HOLDING', '/gcm/where/tpcedata/10kdata/flat_out/Holding.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','HOLDING_HISTORY', '/gcm/where/tpcedata/10kdata/flat_out/HoldingHistory.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','WATCH_LIST', '/gcm/where/tpcedata/10kdata/flat_out/WatchList.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','WATCH_ITEM', '/gcm/where/tpcedata/10kdata/flat_out/WatchItem.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CASH_TRANSACTION', '/gcm/where/tpcedata/10kdata/flat_out/CashTransaction.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CHARGE', '/gcm/where/tpcedata/10kdata/flat_out/Charge.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','COMMISSION_RATE', '/gcm/where/tpcedata/10kdata/flat_out/CommissionRate.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--exit;
