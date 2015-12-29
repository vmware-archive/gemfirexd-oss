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
package management.operations;

import hydra.BasePrms;
import hydra.TestConfig;

public class OperationPrms extends BasePrms {

  static {
    setValues(OperationPrms.class);
  }

  /* Region Params */

  public static Long regionList;

  public static Long regionCacheListner;

  public static Long regionCacheLoader;

  public static Long regionCacheWriter;

  public static Long regionOperations;

  public static Long recordRegionOperations;

  public static Long recordEntryOperations;
  public static Long recordTxOperations;
  public static Long recordDlockOperations;

  public static boolean recordRegionOperations() {
    return TestConfig.tab().booleanAt(recordRegionOperations, false);
  }

  public static boolean recordEntryOperations() {
    return TestConfig.tab().booleanAt(recordEntryOperations, false);
  }

  public static boolean recordTxOperations() {
    return TestConfig.tab().booleanAt(recordTxOperations, false);
  }
  
  public static boolean recordDlockOperations() {
    return TestConfig.tab().booleanAt(recordDlockOperations, false);
  }

  /* EntryOperations Prms */

  /**
   * (Vector of Strings) A list of the operations on a region entry that this
   * test is allowed to do. Can be one or more of: add - add a new key/value to
   * a region. invalidate - invalidate an entry in a region. localInvalidate -
   * local invalidate of an entry in a region. destroy - destroy an entry in a
   * region. localDestroy - local destroy of an entry in a region. update -
   * update an entry in a region with a new value. read - read the value of an
   * entry.
   */
  public static Long entryOperations;

  /**
   * (int) The lower size of the region that will trigger the test choose its
   * operations from the lowerThresholdOperations.
   */

  public static Long lowerThreshold;

  /**
   * (Vector of Strings) A list of the operations on a region entry that this
   * test is allowed to do when the region size falls below lowerThreshold.
   */

  public static Long lowerThresholdOperations;

  /**
   * (int) The upper size of the region that will trigger the test chooose its
   * operations from the upperThresholdOperations.
   */

  public static Long upperThreshold;

  public static Long upperThresholdOperations;
  /**
   * (Vector of Strings) A list of operations on a region that this test is
   * allowed to do when the region size exceeds the upperThreshold.
   */

  /* QueryOperations Prms */

  /**
   * This param **overrides** entryOperations when QueryOperations is used. Add
   * more query operations in list so that test is favored towards querying
   */

  public static Long entryANDQueryOperations;
  public static Long recordQueryOperations;

  public static boolean recordQueryOperations() {
    return TestConfig.tab().booleanAt(recordQueryOperations, false);
  }

  /* Generic Prms */

  public static Long regionLoadSize;

  /**
   * Valid Strings : see
   * ObjectHelper.createObject(objects.ArrayOfByte,SizedString,
   * BatchString,TestInteger and ConfigurableObject) and ValueHolder Default is
   * : ValueHolder For QueryOperations use any implementation of
   * ConfigurableObject with ID field(objects.Portfolio)
   */
  public static Long objectType;

  public static String getObjectType() {
    String type = TestConfig.tab().stringAt(objectType, "valueHolder");
    return type;
  }

  /* Transaction Prms */

  public static Long txCommitPercentage;

  public static Long useTransactions;

  public static boolean useTransactions() {
    return TestConfig.tab().booleanAt(useTransactions, false);
  }

  public static int getCommitPercentage() {
    return TestConfig.tab().intAt(txCommitPercentage, 90);
  }

  /* Function Prms */

  public static Long recordFunctionOps;

  public static Long functionOps;

  /**
   * How many functions to register. One type of each function is registered you
   * can repeat sleep function
   */
  public static Long functionRegisterList;

  /** What kind of distribution you want in your functions */
  public static Long functionList;

  public static Long functionSleepDelay;

  public static boolean recordFunctionOps() {
    return TestConfig.tab().booleanAt(recordFunctionOps, false);
  }

  /* Continuous Query and Index Operations Prms */
  public static Long recordCqIndexOps;

  public static Long cqIndexOps;

  public static boolean recordCqIndexOps() {
    return TestConfig.tab().booleanAt(recordCqIndexOps, false);
  }
  
  /* DLOCk Prms*/
  public static Long dlockWaitTimeMillis;
  public static long dlockLeaseTimeMillis;
  public static long dlockDelay;

 

}
