================================================================================
OVERVIEW:
This describes how to run tests for the TPC-H benchmark.

TPC-H is a benchmark to measure query performance. There are 22 queries specified by TPC-H.
In addition, TPC-H provides a data generator to generate meaningful data for the queries.

TPC-H provides query result answers for each of the 22 queries when they are run with
TPC-H specified query parameters on 1G of generated data.

The data generator has already been run for certain data sizes such as 1G, so you might 
not need to run it. Details are below.

================================================================================
HOW TO RUN QUERY VALIDATION:
This step outlines how to run validation on all TPC-H queries on 1G of data.
This step allows you to gain confidence in the query results and does not measure
performance.

The data size of 1G has already been generated and resides in the WDC. The validation 
bt is configured to find it and use it as long as it is running in the WDC.

Query validation bts are single host, and you do NOT need to edit the local.conf files
listed below.

To run query validation on gfxd, run this bt:
<gfxd-product-checkout>/tests/gfxdperf/tpch/gfxd/validate/validate_gfxd.bt
with this local.conf:
<gfxd-product-checkout>/tests/gfxdperf/tpch/gfxd/validate/local.validate.conf

To run query validation on oracle, you must run on a machine with oracle installed.
As of the time of this writing only w2-2013-lin-15 in the WDC has oracle, and the
local.conf is configured for this machine.
Run this bt:
<gfxd-product-checkout>/tests/gfxdperf/tpch/oracle/validate/validate_oracle.bt
with this local.conf:
<gfxd-product-checkout>/tests/gfxdperf/tpch/oracle/validate/local.validate.conf

================================================================================
HOW TO RUN THE DATA GENERATOR:
The executable for the data generator is checked in to <gfxd-product-checkout>/tests/gfxdperf/tpch/dbgen.
The executable name in that directory is dbgen.
You must cd to <gfxd-product-checkout>/tests/gfxdperf/tpch/dbgen to run it as there is a
file, dists.dss, checked in to that directory that dbgen expects be in the directory
where it is run from.

You can run help with:
./dbgen -help

To generate 1G of data:
cd to <gfxd-product-checkout>/tests/gfxdperf/tpch/dbgen
./dbgen -vf -s 1

The result is a series of *.tbl files containing data for each of the TPC-H tables:
76 % ls -l *.tbl
-rw-r--r-- 1 lynn users  24346144 Sep  4 11:19 customer.tbl
-rw-r--r-- 1 lynn users 759863287 Sep  4 11:19 lineitem.tbl
-rw-r--r-- 1 lynn users      2224 Sep  4 11:20 nation.tbl
-rw-r--r-- 1 lynn users 171952161 Sep  4 11:19 orders.tbl
-rw-r--r-- 1 lynn users 118984616 Sep  4 11:20 partsupp.tbl
-rw-r--r-- 1 lynn users  24135125 Sep  4 11:20 part.tbl
-rw-r--r-- 1 lynn users       389 Sep  4 11:20 region.tbl
-rw-r--r-- 1 lynn users   1409184 Sep  4 11:19 supplier.tbl
77 % wc *.tbl
    150000    1653059   24346144 customer.tbl
   6001215   35481203  759863287 lineitem.tbl
        25        276       2224 nation.tbl
   1500000   11467545  171952161 orders.tbl
    800000   13936288  118984616 partsupp.tbl
    200000    1959094   24135125 part.tbl
         5         50        389 region.tbl
     10000      96975    1409184 supplier.tbl
   8661245   64594490 1100693130 total



