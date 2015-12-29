/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package gfxdperf.tpch.oracle;

import gfxdperf.tpch.AbstractQ7;

import java.sql.Connection;
import java.util.Random;

/**
 * Encapsulates TPC-H Q7.
 */
public class Q7 extends AbstractQ7 {

  public Q7(Connection conn, Random rng) {
    super();
    this.connection = conn;
    this.rng = rng;
    // removed "as shipping"
    this.query = "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = ? and n2.n_name = ?) or (n1.n_name = ? and n2.n_name = ?)) and l_shipdate between date '1995-01-01' and date '1996-12-31') group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year";
  }
}
