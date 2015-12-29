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

package parReg;

import hydra.*;
import com.gemstone.gemfire.admin.*;

public class ParRegAlertListener implements AlertListener {

/** Record an error into the blackboard shared map (so clients can check for it)
 *  if we are the first one to get an error. Any subsequent errors are ignored
 *  because we want to record the first error.
 *
 *  @param error A string containing an error
 */
private void recordErrorIfFirst(String error) {
   Log.getLogWriter().info("Detected " + error);
   long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.ErrorRecorded);
   if (counter == 1) { // we are the first to encounter an error; record it in the blackboard
      Log.getLogWriter().info("Putting error " + error + " into blackboard map at key " + ParRegBB.ErrorKey + " for client to notice");
      ParRegBB.getBB().getSharedMap().put(ParRegBB.ErrorKey, error);
   }
}

public void alert(Alert alert) {
   int myPID = ProcessMgr.getProcessId();
   String key = ParRegTest.VmIDStr + RemoteTestModule.getMyVmid();
   String message = alert.getMessage();
   StringBuffer aStr = new StringBuffer();
   aStr.append("Invoked " + this.getClass().getName() + " in client with vmID " + RemoteTestModule.getMyVmid() + ", pid " + myPID + "\n");
   aStr.append("   alert.getConnectionName(): " + alert.getConnectionName() + "\n");
   aStr.append("   alert.getDate(): " + alert.getDate() + "\n");
   aStr.append("   alert.getLevel(): " + alert.getLevel() + "\n");
   aStr.append("   alert.getMessage(): " + message + "\n");
   aStr.append("   alert.getSourceId(): " + alert.getSourceId() + "\n");
   aStr.append("   alert.getSystemMember(): " + alert.getSystemMember());
   Log.getLogWriter().info(aStr.toString());

   // check that the message is one for LOCAL_MAX_MEMORY
   boolean isLocalMaxMemoryMsg = message.indexOf(FillTest.ExceededLocalMaxMemoryMsg) >= 0;
   if (!isLocalMaxMemoryMsg) { 
      boolean is15SecWarning = message.indexOf("15 seconds have elapsed while waiting") >= 0;
      if (is15SecWarning) { // ignore 15 second warnings
         Log.getLogWriter().info("Ignoring " + message);
         return;
      }
      recordErrorIfFirst("Unexpected alert message: " + aStr);
      return;
   }

   // get the number of MB reported by this alert
   int index1 = message.indexOf(" Mb");
   int index2 = message.lastIndexOf(" ", index1-1); 
   int maxMemoryForThisAlert = (Integer.valueOf(message.substring(index2+1, index1))).intValue();
   Log.getLogWriter().info("MB for this alert: " + maxMemoryForThisAlert);

   // bump the appropriate blackboard counter
   String counterName = FillTest.CounterPrefix + maxMemoryForThisAlert;
   int counter = ParRegBB.getBB().getSharedCounter("AlertForLocalMaxMemory" + maxMemoryForThisAlert);
   long value = ParRegBB.getBB().getSharedCounters().incrementAndRead(counter);
   Log.getLogWriter().info("Counter " + counterName + " is now " + value); 
}

}
