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

package util;

import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.admin.*;
import java.util.*;

/**
 *
 * A class used to store keys for Utility OperationsClient
 *
 */

public class OperationsClientPrms extends BasePrms {

/** (boolean) True if the test should use locking.
 */
public static Long lockOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do, as long as the global region
 *                      size is < regionSizeThreshold.
 */
public static Long entryOperations;  

/** (int) The size of the partitioned region that will trigger the
 *        test to choose its operations from lowerThresholdOperations.
 */
public static Long lowerThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold.
 */
public static Long lowerThresholdOperations;  

/** (int) The upper size of the partitioned region that will trigger the
 *        test to choose its operations from upperThresholdOperations.
 */
public static Long upperThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdOperations;  

/** (int) The global size of the partitioned region that will trigger the
 *        test to choose its operations from thresholdOperations.
 */
public static Long numOpsPerTask;  
public static int numOpsPerTask() {
  Long key = numOpsPerTask;
  return tasktab().intAt( key, tab().intAt( key, Integer.MAX_VALUE));
}

/** (boolean) True if the test execute operations within a single transaction
 *  Defaults to false
 */
public static Long useTransactions;
public static boolean useTransactions() {
  Long key = useTransactions;
  return tasktab().booleanAt( key, tab().booleanAt( key, false ));
}

/** (int) In transaction tests, the percentage of transactions to perform commit
 *  (vs. rollback).  Default = 100%.
 */
public static Long commitPercentage;
public static int getCommitPercentage() {
  Long key = commitPercentage;
  return tasktab().intAt( key, tab().intAt( key, 100 ));
}

/** (List) A List of class names of the objects to put into the cache.
 */
public static Long objectTypes;
public static List getObjectTypes() {
  Long key = objectTypes;
  return tasktab().vecAt( key, tab().vecAt( key, null ));
}

// ================================================================================
static {
   BasePrms.setValues(OperationsClientPrms.class);
}

}
