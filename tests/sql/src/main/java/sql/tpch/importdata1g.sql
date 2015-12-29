SET SCHEMA TPCHGFXD;

CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'NATION', '/gcm/where/tpchdata/1Gdata/nation.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'REGION', '/gcm/where/tpchdata/1Gdata/region.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'PART', '/gcm/where/tpchdata/1Gdata/part.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'SUPPLIER', '/gcm/where/tpchdata/1Gdata/supplier.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'PARTSUPP', '/gcm/where/tpchdata/1Gdata/partsupp.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'CUSTOMER', '/gcm/where/tpchdata/1Gdata/customer.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'ORDERS', '/gcm/where/tpchdata/1Gdata/orders.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'LINEITEM', '/gcm/where/tpchdata/1Gdata/lineitem.tbl', '|', null, null, 0, 0, 4, 0, null, null);

--CREATE TABLE TEMP_Q1 AS SELECT * FROM LINEITEM WITH NO DATA;
--insert into TEMP_Q1 select * from LINEITEM where l_shipdate <= cast({fn timestampadd(SQL_TSI_DAY, -90, timestamp('1998-12-01 23:59:59'))} as DATE);

