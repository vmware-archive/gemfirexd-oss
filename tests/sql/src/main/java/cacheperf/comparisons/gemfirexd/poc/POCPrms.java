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

public class POCPrms extends BasePrms {

  static {
    setValues(POCPrms.class);
  }

  public static enum POCType {
   UseCase3,
   TelcelLoyalty,
   UseCase2;
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
   * POC type.
   */
  public static Long pocType;
  public static POCType getPOCType() {
    Long key = pocType;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val.equalsIgnoreCase(POCType.UseCase3.toString())) {
      return POCType.UseCase3;
    } else if (val.equalsIgnoreCase(POCType.TelcelLoyalty.toString())) {
      return POCType.TelcelLoyalty;
    } else if (val.equalsIgnoreCase(POCType.UseCase2.toString())) {
      return POCType.UseCase2;
    } else {
      String s = "Unrecognized " + BasePrms.nameForKey(pocType) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (int)
   * Number of batches of data to load.
   */
  public static Long numBatches;
  public static int getNumBatches() {
    Long key = numBatches;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val <= 0) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required and must be positive";
      throw new HydraConfigException(s);
    }
    return val;
  }
  /**
   * (int)
   * Size of batches of data to load.
   */
  public static Long batchSize;
  public static int getBatchSize() {
    Long key = batchSize;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val <= 0) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required and must be positive";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Percentage of operations that are updates.
   */
  public static Long updatePercentage;
  public static int getUpdatePercentage() {
    Long key = updatePercentage;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0) {
      String s = BasePrms.nameForKey(ddlFileName) + " is required and must be non-negative";
      throw new HydraConfigException(s);
    }
    return val;
  }
}
