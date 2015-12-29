SET SCHEMA TPCHGFXD;

CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'NATION', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/nation.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'REGION', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/region.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'PART', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/part.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'SUPPLIER', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/supplier.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'PARTSUPP', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/partsupp.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'CUSTOMER', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/customer.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'ORDERS', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/orders.tbl', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TPCHGFXD', 'LINEITEM', '/export/w2-2013-lin-22a/users/royc/tpchdata/tpch_100G/lineitem.tbl', '|', null, null, 0, 0, 4, 0, null, null);

