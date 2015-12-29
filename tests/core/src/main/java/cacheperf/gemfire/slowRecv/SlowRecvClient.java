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

package cacheperf.gemfire.slowRecv;

import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;

import hydra.*;
import util.*;

/**
 *  Additional slow receiver tasks to validate queuing, etc occurred
 */

public class SlowRecvClient extends cacheperf.CachePerfClient {

   /**  
    *  CLOSETASK to validate that distributionTimeout occured, 
    *  queuing and queue flush performed
    */
   public static void verifySlowReceiverDetectedTask() {
      SlowRecvClient sr = new SlowRecvClient();
      sr.initHydraThreadLocals();
      sr.verifySlowReceiverDetected();
      sr.updateHydraThreadLocals();
   }

   /**  verifies we queued messages (if distribution-timeout != 0)
    */
   protected void verifySlowReceiverDetected() {

     // don't perform validation if we aren't expecting queuing or
     // asyncDistributionTimeout == 0
     Long key = hydra.GemFirePrms.asyncDistributionTimeout;
     int distributionTimeout = TestConfig.tasktab().intAt( key, TestConfig.tab().intAt( key, 0));
     if (distributionTimeout == 0) {
       Log.getLogWriter().info("asyncDistributionTimeout == 0, no async messaging configured");
       return;
     }

     // todo@lhughes -- verify queing did NOT occur and throw exception if it did
     if (!SlowRecvPrms.expectQueuing()) {
       Log.getLogWriter().info("SlowRecvPrms.expectQueuing is set to false, no checks done to ensure queuing occurred");
       return;
     }
     
     StringBuffer aStr = new StringBuffer();

     // get a handle on the DistributionManager stats
     DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
     DM dm = ((InternalDistributedSystem)ds).getDistributionManager();
     DMStats dmStats = dm.getStats();

     // asyncQueuedMessages
     long queuedMessages = dmStats.getAsyncQueuedMsgs();
     Log.getLogWriter().info("asyncQueuedMsgs = " + queuedMessages);
     aStr.append("asyncQueuedMsgs = " + queuedMessages + "\n");

     // asyncFlushesCompleted
     int flushesCompleted = dmStats.getAsyncQueueFlushesCompleted();
     Log.getLogWriter().info("queueFlushesCompleted = " + flushesCompleted);
     aStr.append("asyncQueueFlushesCompleted = " + flushesCompleted + "\n");

     if ((queuedMessages <= 0) || (flushesCompleted <= 0)) {
        throw new TestException("TuningRequired: Test did not detect async messaging queuing, stats = \n" + aStr.toString());
     }
   }
}
