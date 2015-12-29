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

package asyncMsg;

import hydra.*;
import util.*;
import com.gemstone.gemfire.admin.*;

public class AdminListener implements AlertListener {

// save the first slowReceiver alert here
private static Alert slowReceiverAlert = null;

/** Log the alert, and save it in the blackboard if it is a slow receiver alert.
 */
public void alert(Alert alert) {
   try {
      String alertStr = getAlertString(alert);
      Log.getLogWriter().info(alertStr);
      lookForSlowReceiverAlert(alert, alertStr);
   } catch (Exception e) {
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.ErrorKey, errStr);
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
      AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.ErrorKey, errStr);
      aStr.append(errStr);
   }
   return aStr.toString();
}

/** If the given alert is a slow receiver alert, record it in the blackboard.
 */
private static synchronized void lookForSlowReceiverAlert(Alert alert, String alertStr) {
   if (slowReceiverAlert == null) {
      String msg = alert.getMessage();
      if (msg.indexOf("asking slow receiver") >= 0) {
         slowReceiverAlert = alert;
         AsyncMsgBB.getBB().getSharedMap().put(AsyncMsgBB.SlowReceiverAlertKey, alertStr);
         Log.getLogWriter().info("Admin VM has recognized slow receiver alert " + alertStr +
                                 " and written this alert to the AsyncMsgBB blackboard");
      }
   }
}

/** Wait for a slow receiver alert to be recognized.
 */
public static void waitForSlowReceiverAlert(int secondsToWait) {
   Log.getLogWriter().info("Waiting to recognize a slow receiver alert...");
   long start = System.currentTimeMillis();
   long millisToWait = secondsToWait * 1000;
   while (System.currentTimeMillis() - start < millisToWait) {
      String alertStr = (String)(AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.SlowReceiverAlertKey));
      if (alertStr != null) {
         return;
      }
      MasterController.sleepForMs(1000);
   }
   throw new TestException("Did not find a slow receiver alert in " + secondsToWait + " seconds");
}

/** Return true if a slow receiver alert has occurred, false otherwise
 */
public static boolean slowReceiverAlertOccurred() {
   Object anObj = AsyncMsgBB.getBB().getSharedMap().get(AsyncMsgBB.SlowReceiverAlertKey);
   return (anObj != null);
}

}
