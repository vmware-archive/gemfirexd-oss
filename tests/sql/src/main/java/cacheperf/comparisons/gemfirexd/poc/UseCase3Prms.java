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
package cacheperf.comparisons.gemfirexd.poc;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class UseCase3Prms extends BasePrms {

  static {
    setValues(UseCase3Prms.class);
  }

  /**
   * (String)
   * Name of table to load.
   */
  public static Long tableName;
  public static String getTableName() {
    Long key = tableName;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * (int)
   * Number of rows to load.
   */
  public static Long rowCount;
  public static int getRowCount() {
    Long key = rowCount;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * (int)
   * Number of import threads to use. Defaults to 6.
   */
  public static Long importThreads;
  public static int getImportThreads() {
    Long key = importThreads;
    return tasktab().intAt(key, tab().intAt(key, 6));
  }
}
