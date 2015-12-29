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

package splitBrain;

import hydra.*;
import util.*;
import com.gemstone.gemfire.admin.*;

public class PRAckAlertListener extends AckAlertListener {

/** Check that the wait time in the alert message is acceptable.
 *  
 *  @param waitTimeInMessage The wait time from the alert message.
 *  @param msg The alert message.  
 */
protected String checkWaitTime(int waitTimeInMessage, String msg) {
   // see if the wait was long enough
   int ackSevereAlertThreshold = TestConfig.tab().intAt(GemFirePrms.ackSevereAlertThreshold);
   int ackWaitThreshold = TestConfig.tab().intAt(GemFirePrms.ackWaitThreshold);
   int sum = ackWaitThreshold + ackSevereAlertThreshold;
   int limit = ackWaitThreshold + (int)(ackSevereAlertThreshold * 0.8);
   if (waitTimeInMessage < limit) { // check if wait is long enough to honor ackWaitThreshold and ackSevereAlertThreshold
      String errStr = "ackWaitThreshold is " + ackWaitThreshold + ", ackSevereAlertThreshold is " +
         ackSevereAlertThreshold + ", sum is " + sum + ", limit for acceptance is " + limit +
         ", expected " + msg + " to wait at least " + limit + " seconds";
      return errStr;
   } 
   return null;
}

}
