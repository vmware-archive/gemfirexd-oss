
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

package versioning;

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.util.*;

/**
 * Hydra parameters for the versioning (conflicted ops) tests.
 *
 * @author lhughes
 * @since 7.0
 */
public class ConflictedOpsPrms extends hydra.BasePrms {

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do, as long as the global region
 *                      size is < regionSizeThreshold.
 */
public static Long entryOperations;  

/** (int) The size of the region that will trigger the
 *        test to choose its operations from lowerThresholdOperations.
 */
public static Long lowerThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold.
 */
public static Long lowerThresholdOperations;  

/** (int) The upper size of the region that will trigger the
 *        test to choose its operations from upperThresholdOperations.
 */
public static Long upperThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdOperations;  

/** (int) The number of operations to perform in a task.
 */
public static Long numOpsPerTask;  

/** (int) The number of seconds to run a test (currently used for concParRegHA)
 *        to terminate the test. We cannot use hydra's taskTimeSec parameter
 *        because of a small window of opportunity for the test to hang due
 *        to the test's "concurrent round robin" type of strategy.
 */
public static Long secondsToRun;

   static {
       setValues( ConflictedOpsPrms.class );
   }
}
