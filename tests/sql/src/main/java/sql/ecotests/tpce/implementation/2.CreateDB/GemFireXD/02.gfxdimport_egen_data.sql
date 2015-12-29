-- Import TPC-E GenLoader generted Data to TPC-E GemFireXD Schema. Do not need converters.
-- BLOB in NEWS_ITEM is actually the ASCII char string
-- Holding summary is a view of holding now
connect client '10.150.30.37:7710;user=tpcegfxd;password=tpcegfxd';
SET SCHEMA TPCEGFXD;
CALL SQLJ.INSTALL_JAR('/s2qa/tangc/samples/tpce/implementation/3.MigrateDB/oraconverter/dataconverters.jar', 'SYSCS_UTIL.converter',0);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','ZIP_CODE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/ZipCode.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','ADDRESS', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Address.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','STATUS_TYPE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/StatusType.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TAXRATE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Taxrate.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CUSTOMER', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Customer.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','EXCHANGE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Exchange.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','SECTOR', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Sector.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','INDUSTRY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Industry.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','COMPANY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Company.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','COMPANY_COMPETITOR', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CompanyCompetitor.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','SECURITY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Security.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','DAILY_MARKET', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/DailyMarket.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','FINANCIAL', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Financial.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
-- contain real timestamp type of data
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','LAST_TRADE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/LastTrade.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','NEWS_ITEM', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/NewsItem.txt', '|', NULL, NULL, 0, 0, 1, 0, 'ASCIICharBlobImportOra', NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','NEWS_XREF', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/NewsXRef.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','BROKER', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Broker.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CUSTOMER_ACCOUNT', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CustomerAccount.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','ACCOUNT_PERMISSION', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/AccountPermission.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CUSTOMER_TAXRATE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CustomerTaxrate.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TRADE_TYPE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/TradeType.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TRADE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Trade.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','SETTLEMENT', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Settlement.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','TRADE_HISTORY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/TradeHistory.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','HOLDING_SUMMARY_TBL', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/HoldingSummary.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','HOLDING', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Holding.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','HOLDING_HISTORY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/HoldingHistory.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','WATCH_LIST', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/WatchList.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','WATCH_ITEM', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/WatchItem.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
--
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CASH_TRANSACTION', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CashTransaction.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','CHARGE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Charge.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
CALL SYSCS_UTIL.IMPORT_TABLE_EX('TPCEGFXD','COMMISSION_RATE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CommissionRate.txt', '|', NULL, NULL, 0, 0, 6, 0, NULL, NULL);
exit;