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
package cacheperf.comparisons.gemfirexd.useCase5.poc;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class UseCase5Prms extends BasePrms {

  static {
    setValues(UseCase5Prms.class);
  }

  /**
   * (int)
   * Number of terminals. Defaults to 1000.
   */
  public static Long numTerminals;
  public static int getNumTerminals() {
    Long key = numTerminals;
    return tab().intAt(key, 1000);
  }

  /**
   * (String)
   * DDL file to use.
   */
  public static Long ddlFile;
  public static String getDDLFile() {
    Long key = ddlFile;
    return tasktab().stringAt(key, tab().stringAt(key));
  }

  /**
   * (int)
   * Number of buckets per server.
   */
  public static Long numBuckets;
  public static int getNumBuckets() {
    Long key = numBuckets;
    return tab().intAt(key, 113);
  }

  /**
   * (boolean)
   * Whether to record statement statistics.  Defaults to false.
   */
  public static Long timeStmts;
  public static boolean timeStmts() {
    Long key = timeStmts;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
}
