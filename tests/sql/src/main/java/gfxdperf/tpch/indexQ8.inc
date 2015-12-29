gfxdperf.tpch.TPCHPrms-indexes = 
  "CREATE INDEX q8_index1 ON part (p_partkey)"
  "CREATE INDEX q8_index2 ON supplier (s_suppkey)"
  "CREATE INDEX q8_index3 ON lineitem (l_suppkey)"
  "CREATE INDEX q8_index4 ON lineitem (l_orderkey)"
  "CREATE INDEX q8_index5 ON orders (o_orderkey)"
  "CREATE INDEX q8_index6 ON orders (o_custkey)"
  "CREATE INDEX q8_index7 ON customer (c_custkey)"
  "CREATE INDEX q8_index8 ON customer (c_nationkey)"
  "CREATE INDEX q8_index9 ON nation (n_nationkey)"
  "CREATE INDEX q8_index10 ON nation (n_regionkey)"
  "CREATE INDEX q8_index11 ON region (r_regionkey)"
  "CREATE INDEX q8_index12 ON region (r_name)"
;
