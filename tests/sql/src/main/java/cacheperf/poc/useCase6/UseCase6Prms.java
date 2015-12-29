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
package cacheperf.poc.useCase6;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class UseCase6Prms extends BasePrms {

  static {
    setValues(UseCase6Prms.class);
  }

  /**
   * (String)
   * Type of table to use: "partitioned" or "replicated".
   */
  public static Long tableType;
  public static String getTableType() {
    Long key = tableType;
    return tab().stringAt(key);
  }

  /**
   * (int)
   * Number of redundant copies for partitioned tables.
   * Ignored if the table is not partitioned.
   */
  public static Long redundantCopies;
  public static int getRedundantCopies() {
    Long key = redundantCopies;
    return tab().intAt(key);
  }

  /**
   * (boolean)
   * Whether to really create indexes in the index task.
   */
  public static Long createIndexes;
  public static boolean createIndexes() {
    Long key = createIndexes;
    return tasktab().booleanAt(key, tab().booleanAt(key));
  }

  public static Long isValue;
  public static boolean isValue() {
    Long key = isValue;
    return tasktab().booleanAt(key, tab().booleanAt(key));
  }

  /**
   * (int)
   * Desired size of each query result set.  Defaults to 1.
   *
   * This is the number of Cashflows per Transact.  The number of Transact
   * is controlled by {@link cacheperf.CachePerfPrms#maxKeys}.  The total
   * number of rows in the two tables is the product of the two parameters.
   */
  public static Long resultSetSize;
  public static int getResultSetSize() {
    Long key = resultSetSize;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(resultSetSize) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  public static Long usePreparedStatements;
  public static boolean usePreparedStatements() {
    Long key = usePreparedStatements;
    return tasktab().booleanAt(key, tab().booleanAt(key));
  }
}
