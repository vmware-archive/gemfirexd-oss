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

package rebalance;

import hydra.*;
import util.*;
import com.gemstone.gemfire.admin.*;

public class CapacityAdminListener implements AlertListener {

public static String lmmExceededKey = "local max memory exceeded mb ";

/** Log the alert, and save it in the blackboard if it is a slow receiver alert.
 */
public void alert(Alert alert) {
   try {
      String alertStr = getAlertString(alert);
      Log.getLogWriter().info(alertStr);
      lookForLMMExceeded(alert);
   } catch (Exception e) {
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      RebalanceBB.getBB().getSharedMap().put(RebalanceBB.ErrorKey, errStr);
   }
}

/** Return a String representation of the given alert.
 */
protected String getAlertString(Alert alert) {
   int myPID = ProcessMgr.getProcessId();
   StringBuffer aStr = new StringBuffer();
   aStr.append("Invoked " + this.getClass().getName() + " in client with vmID " + RemoteTestModule.getMyVmid() + ", pid " + myPID + "\n");
   aStr.append("   alert.getConnectionName(): " + alert.getConnectionName() + "\n");
   aStr.append("   alert.getDate(): " + alert.getDate() + "\n");
   aStr.append("   alert.getLevel(): " + alert.getLevel() + "\n");
   aStr.append("   alert.getMessage(): " + alert.getMessage() + "\n");
   aStr.append("   alert.getSourceId(): " + alert.getSourceId() + "\n");
   try {
      aStr.append("   alert.getSystemMember(): " + alert.getSystemMember());
   } catch (OperationCancelledException e) {
      String errStr = "Bug 39657; Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      RebalanceBB.getBB().getSharedMap().put(RebalanceBB.ErrorKey, errStr);
      aStr.append(errStr);
   }
   return aStr.toString();
}

/** Look for an alert that localMaxMemory was exceeded in a vm
 *  and write it to the blackboard
 */
protected void lookForLMMExceeded(Alert alert) {
   String message = alert.getMessage();
   String lmmExceededStr = "has exceeded local maximum memory configuration ";
   int index = message.indexOf(lmmExceededStr);
   if (index >= 0) { // this is a local max memory alert
      String subStr = message.substring(index + lmmExceededStr.length(), message.length());
      String[] tokens = subStr.split(" ");
      int mb = Integer.valueOf(tokens[0]);
      String key = lmmExceededKey + mb;
      RebalanceBB.getBB().getSharedMap().put(key, alert.getSystemMember().toString());
      long counter = RebalanceBB.getBB().getSharedCounters().incrementAndRead(RebalanceBB.numExceededLMMAlerts);
      Log.getLogWriter().info("Alert for localMaxMemory exceeded " + mb + " MB was recognized, " +
          "counter is now " + counter);
   }
}

}
