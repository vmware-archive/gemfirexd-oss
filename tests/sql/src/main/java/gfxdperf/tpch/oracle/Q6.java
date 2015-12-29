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

import gfxdperf.tpch.AbstractQ6;

import java.sql.Connection;
import java.util.Random;

/**
 * Encapsulates TPC-H Q6.
 */
public class Q6 extends AbstractQ6 {

  public Q6(Connection conn, Random rng) {
    super();
    this.connection = conn;
    this.rng = rng;
    // this oracle query contains syntax for a date interval (see NUMTOYMINTERVAL)
    this.query = "select sum(l_extendedprice*l_discount) as revenue from lineitem where l_shipdate >= ? and l_shipdate < ? + NUMTOYMINTERVAL(1, 'year') and l_discount between ? - 0.01 and ? + 0.01 and l_quantity < ?";  
  }
}
