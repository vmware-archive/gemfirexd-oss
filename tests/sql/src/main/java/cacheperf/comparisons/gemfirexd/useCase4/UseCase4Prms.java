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
package cacheperf.comparisons.gemfirexd.useCase4;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class UseCase4Prms extends BasePrms {

  static {
    setValues(UseCase4Prms.class);
  }

  public static enum QueryType {
    holdingAgg,
    portSumm,
    mktSumm,
    holdingCount,
    uptClosedOrderSelfJoin,
    uptClosedOrder,
    findOrderByStatus,
    findOrderByAccAccId,
    findOrderIdAndAccAccId,
    findOrderCntAccAccId,
    findOrderCntAccAccIdAndStatus,
    mixed;
  }

  public static final int DEFAULT_ROWS_IN_ACCOUNT = 100;
  public static final int DEFAULT_ROWS_IN_ACCOUNTPROFILE = 100;
  public static final int DEFAULT_ROWS_IN_HOLDING = 10000;
  public static final int DEFAULT_ROWS_IN_ORDERS = 100000;
  public static final int DEFAULT_ROWS_IN_QUOTE = 100;

  public static final String DEFAULT_DDL_FILENAME = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase4/ddl/useCase4.sql";

  public static final String DEFAULT_ACCOUNT_FILENAME = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase4/data/ACCOUNT-100.dat";
  public static final String DEFAULT_ACCOUNTPROFILE_FILENAME = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase4/data/ACCOUNTPROFILE-100.dat";
  public static final String DEFAULT_HOLDING_FILENAME = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase4/data/HOLDING-10000.dat";
  public static final String DEFAULT_ORDERS_FILENAME = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase4/data/ORDERS-100000.dat";
  public static final String DEFAULT_QUOTE_FILENAME = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase4/data/QUOTE-100.dat";

  /**
   * (String)
   * Query type.
   */
  public static Long queryType;
  public static QueryType getQueryType() {
    Long key = queryType;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val.equalsIgnoreCase(QueryType.holdingAgg.toString())) {
      return QueryType.holdingAgg;
    } else if (val.equalsIgnoreCase(QueryType.portSumm.toString())) {
      return QueryType.portSumm;
    } else if (val.equalsIgnoreCase(QueryType.mktSumm.toString())) {
      return QueryType.mktSumm;
    } else if (val.equalsIgnoreCase(QueryType.holdingCount.toString())) {
      return QueryType.holdingCount;
    } else if (val.equalsIgnoreCase(QueryType.uptClosedOrderSelfJoin.toString())) {
      return QueryType.uptClosedOrderSelfJoin;
    } else if (val.equalsIgnoreCase(QueryType.uptClosedOrder.toString())) {
      return QueryType.uptClosedOrder;
    } else if (val.equalsIgnoreCase(QueryType.findOrderByStatus.toString())) {
      return QueryType.findOrderByStatus;
    } else if (val.equalsIgnoreCase(QueryType.findOrderByAccAccId.toString())) {
      return QueryType.findOrderByAccAccId;
    } else if (val.equalsIgnoreCase(QueryType.findOrderIdAndAccAccId.toString())) {
      return QueryType.findOrderIdAndAccAccId;
    } else if (val.equalsIgnoreCase(QueryType.findOrderCntAccAccId.toString())) {
      return QueryType.findOrderCntAccAccId;
    } else if (val.equalsIgnoreCase(QueryType.findOrderCntAccAccIdAndStatus.toString())) {
      return QueryType.findOrderCntAccAccIdAndStatus;
    } else if (val.equalsIgnoreCase(QueryType.mixed.toString())) {
      return QueryType.mixed;
    } else {
      String s = "Unrecognized " + BasePrms.nameForKey(queryType) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (int)
   * Number of rows in the ACCOUNTPROFILE table.
   * Defaults to {@link #DEFAULT_ROWS_IN_ACCOUNTPROFILE}.
   */
  public static Long rowsInAccountProfile;
  public static int getRowsInAccountProfile() {
    Long key = rowsInAccountProfile;
    return tab().intAt(key, DEFAULT_ROWS_IN_ACCOUNTPROFILE);
  }

  /**
   * (int)
   * Number of rows in the ACCOUNT table.
   * Defaults to {@link #DEFAULT_ROWS_IN_ACCOUNT}.
   */
  public static Long rowsInAccount;
  public static int getRowsInAccount() {
    Long key = rowsInAccount;
    return tab().intAt(key, DEFAULT_ROWS_IN_ACCOUNT);
  }

  /**
   * (int)
   * Number of rows in the HOLDING table.
   * Defaults to {@link #DEFAULT_ROWS_IN_HOLDING}.
   */
  public static Long rowsInHolding;
  public static int getRowsInHolding() {
    Long key = rowsInHolding;
    return tab().intAt(key, DEFAULT_ROWS_IN_HOLDING);
  }

  /**
   * (int)
   * Number of rows in the ORDERS table.
   * Defaults to {@link #DEFAULT_ROWS_IN_ORDERS}.
   */
  public static Long rowsInOrders;
  public static int getRowsInOrders() {
    Long key = rowsInOrders;
    return tab().intAt(key, DEFAULT_ROWS_IN_ORDERS);
  }

  /**
   * (int)
   * Number of rows in the QUOTE table.
   * Defaults to {@link #DEFAULT_ROWS_IN_QUOTE}.
   */
  public static Long rowsInQuote;
  public static int getRowsInQuote() {
    Long key = rowsInQuote;
    return tab().intAt(key, DEFAULT_ROWS_IN_QUOTE);
  }

  /**
   * (String)
   * Name of ddl file to use.
   */
  public static Long ddlFileName;
  public static String getDDLFileName() {
    Long key = ddlFileName;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (String)
   * Name of data file to use for the Account table.
   */
  public static Long dataFileNameForAccount;
  public static String getDataFileNameForAccount() {
    Long key = dataFileNameForAccount;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (String)
   * Name of data file to use for the AccountProfile table.
   */
  public static Long dataFileNameForAccountProfile;
  public static String getDataFileNameForAccountProfile() {
    Long key = dataFileNameForAccountProfile;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (String)
   * Name of data file to use for the Holding table.
   */
  public static Long dataFileNameForHolding;
  public static String getDataFileNameForHolding() {
    Long key = dataFileNameForHolding;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (String)
   * Name of data file to use for the Orders table.
   */
  public static Long dataFileNameForOrders;
  public static String getDataFileNameForOrders() {
    Long key = dataFileNameForOrders;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (String)
   * Name of data file to use for the Quote table.
   */
  public static Long dataFileNameForQuote;
  public static String getDataFileNameForQuote() {
    Long key = dataFileNameForQuote;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required";
      throw new HydraConfigException(s);
    }
    return val;
  }
}
