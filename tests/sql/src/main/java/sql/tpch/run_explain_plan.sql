SET SCHEMA TPCHGFXD;

--Q1 explain plan

explain select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,    sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,    avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc,  count(*) as count_order from    lineitem where    l_shipdate <= cast({fn timestampadd(SQL_TSI_DAY, -90, timestamp('1998-12-01 23:59:59'))} as DATE) group by    l_returnflag, l_linestatus order by    l_returnflag, l_linestatus;

--Q2 explain plan

explain select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from   part , partsupp, supplier, nation, region where   p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS'   and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'   and ps_supplycost = (     select min(ps_supplycost)     from       part, partsupp, supplier, nation, region     where       p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey       and r_name = 'EUROPE'   ) order by   s_acctbal desc, n_name, s_name, p_partkey;

--Q3 eplain plan

explain select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from   customer, orders, lineitem where   c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey   and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by   l_orderkey, o_orderdate, o_shippriority order by   revenue desc, o_orderdate;

--Q4 explain plan

explain select o_orderpriority, count(*) as order_count from   orders where   o_orderdate >= '1993-07-01'   and o_orderdate < cast({fn timestampadd(SQL_TSI_MONTH, 3, timestamp('1993-07-01 23:59:59'))} as DATE)   and exists (     select *     from      lineitem     where       l_orderkey = o_orderkey and l_commitdate < l_receiptdate   ) group by   o_orderpriority order by   o_orderpriority;

--Q4 explain plan

explain select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from   customer, orders, lineitem, supplier, nation, region where   c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA'   and o_orderdate >= '1994-01-01'   and o_orderdate < cast({fn timestampadd(SQL_TSI_YEAR, 1, timestamp('1994-01-01 23:59:59'))} as DATE) group by   n_name order by   revenue desc;


