gfxdperf.tpch.TPCHPrms-indexes = 
  "CREATE INDEX q4_index1 ON orders (o_orderdate)"
  "CREATE INDEX q4_index2 ON orders (o_orderkey)"
  "CREATE INDEX q4_index3 ON lineitem (l_orderkey)"
  "CREATE INDEX q4_index4 ON lineitem (l_orderkey)"
  "CREATE INDEX q4_index5 ON lineitem (l_commitdate)"
  "CREATE INDEX q4_index6 ON lineitem (l_receiptdate)"
;
