gfxdperf.tpch.TPCHPrms-indexes = 
  "CREATE INDEX q7_index1 ON supplier (s_suppkey)"
  "CREATE INDEX q7_index2 ON orders (o_orderkey)"
  "CREATE INDEX q7_index3 ON lineitem (l_orderkey)"
  "CREATE INDEX q7_index4 ON customer (c_custkey)"
  "CREATE INDEX q7_index5 ON orders (o_custkey)"
  "CREATE INDEX q7_index6 ON supplier (s_nationkey)"
  "CREATE INDEX q7_index7 ON customer (c_nationkey)"
  "CREATE INDEX q7_index8 ON nation (n_name)"
  "CREATE INDEX q7_index9 ON lineitem (l_shipdate)"
;
