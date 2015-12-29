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
package cacheperf.comparisons.gemfirexd.misc;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class CarterPrms extends BasePrms {

  static {
    setValues(CarterPrms.class);
  }

  public static final int DEFAULT_NUM_PRODUCTS = 50;
  public static final int DEFAULT_NUM_STORES = 50;
  public static final int DEFAULT_NUM_SALES_RECORDS = 60000;

  /**
   * (int)
   * Number of products.  Defaults to {@link #DEFAULT_NUM_PRODUCTS};
   */
  public static Long numProducts;
  public static int getNumProducts() {
    Long key = numProducts;
    return tab().intAt(key, DEFAULT_NUM_PRODUCTS);
  }

  /**
   * (int)
   * Number of stores.  Defaults to {@link #DEFAULT_NUM_STORES};
   */
  public static Long numStores;
  public static int getNumStores() {
    Long key = numStores;
    return tab().intAt(key, DEFAULT_NUM_STORES);
  }

  /**
   * (int)
   * Number of sales.  Defaults to {@link #DEFAULT_NUM_SALES_RECORDS};
   */
  public static Long numSalesRecords;
  public static int getNumSalesRecords() {
    Long key = numSalesRecords;
    return tab().intAt(key, DEFAULT_NUM_SALES_RECORDS);
  }
}
