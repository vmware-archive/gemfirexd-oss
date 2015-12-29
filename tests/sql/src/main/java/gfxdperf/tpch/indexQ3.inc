gfxdperf.tpch.TPCHPrms-indexes = 
  "CREATE INDEX q3_index1 ON customer (c_custkey)"
  "CREATE INDEX q3_index2 ON orders (o_custkey)"
  "CREATE INDEX q3_index3 ON lineitem (l_orderKey)"
  "CREATE INDEX q3_index4 ON orders (o_orderkey)"
  "CREATE INDEX q3_index5 ON orders (o_orderdate)"
  "CREATE INDEX q3_index6 ON lineitem (l_shipdate)"
;
