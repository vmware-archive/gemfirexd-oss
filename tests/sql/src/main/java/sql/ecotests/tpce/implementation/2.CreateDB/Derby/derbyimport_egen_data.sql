-- Need modify the data file directory to point to right location
-- Import TPC-E GenLoader generted Data to TPC-E GemFireXD Schema. Do not need converters.
-- BLOB in NEWS_ITEM is actually the ASCII char string
-- Holding summary is a view of holding now
connect 'jdbc:derby://10.150.30.39:1527/tpce';
-- CALL SQLJ.INSTALL_JAR('/s2qa/tangc/samples/tpce/implementation/3.DBMigration/oraconverter/dataconverters.jar', 'SYSCS_UTIL.converter',0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','ZIP_CODE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/ZipCode.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','ADDRESS', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Address.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','STATUS_TYPE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/StatusType.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','TAXRATE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Taxrate.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','CUSTOMER', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Customer.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','EXCHANGE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Exchange.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','SECTOR', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Sector.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','INDUSTRY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Industry.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','COMPANY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Company.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','COMPANY_COMPETITOR', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CompanyCompetitor.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','SECURITY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Security.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','DAILY_MARKET', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/DailyMarket.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','FINANCIAL', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Financial.txt', '|', NULL, NULL, 0);
-- contain real timestamp type of data
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','LAST_TRADE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/LastTrade.txt', '|', NULL, NULL, 0);
--
-- The blob has the problem in ASCII C++, data could not be populated to NewsItem & NewsXRef
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','NEWS_ITEM', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/NewsItem.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','NEWS_XREF', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/NewsXRef.txt', '|', NULL, NULL, 0);

CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','BROKER', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Broker.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','CUSTOMER_ACCOUNT', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CustomerAccount.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','ACCOUNT_PERMISSION', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/AccountPermission.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','CUSTOMER_TAXRATE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CustomerTaxrate.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','TRADE_TYPE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/TradeType.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','TRADE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Trade.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','SETTLEMENT', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Settlement.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','TRADE_HISTORY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/TradeHistory.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','HOLDING_SUMMARY_TBL', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/HoldingSummary.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','HOLDING', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Holding.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','HOLDING_HISTORY', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/HoldingHistory.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','WATCH_LIST', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/WatchList.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','WATCH_ITEM', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/WatchItem.txt', '|', NULL, NULL, 0);
--
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','CASH_TRANSACTION', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CashTransaction.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','CHARGE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/Charge.txt', '|', NULL, NULL, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE('APP','COMMISSION_RATE', '/s2qa/tangc/samples/tpce/egen/flat_out_c1000_t1000_w10/CommissionRate.txt', '|', NULL, NULL, 0);
exit;