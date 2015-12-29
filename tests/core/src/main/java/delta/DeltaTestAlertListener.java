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

package delta;

import hydra.*;
import util.*;
import com.gemstone.gemfire.admin.*;

public class DeltaTestAlertListener implements AlertListener {

public static final String warningKey = "warning";
public static final String severeKey = "severe";

public void alert(Alert alert) {
   Log.getLogWriter().info(getAlertString(alert));
   saveAlert(alert);
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
   aStr.append("   alert.getSystemMember(): " + alert.getSystemMember());
   return aStr.toString();
}

/** Clear the blackboard of alerts
 */
public static void clearAlerts() {
   Log.getLogWriter().info("Clearing alerts in blackboard...");
   DeltaPropagationBB.getBB().getSharedMap().remove(severeKey);
   DeltaPropagationBB.getBB().getSharedMap().remove(warningKey);
}

/** Save each alert in the blackboard
 */
private void saveAlert(Alert alert) {
   if (alert.getLevel().equals(AlertLevel.SEVERE)) { 
      DeltaPropagationBB.getBB().getSharedMap().put(severeKey, alert.getMessage());
   } else if (alert.getLevel().equals(AlertLevel.WARNING)) { 
      DeltaPropagationBB.getBB().getSharedMap().put(warningKey, alert.getMessage());
   }
}

/** Wait for a warning indicating a put could not complete when delta originates
 *  in server and fromDelta fails in client.
 */
public static void waitForWarning() {
   long millisToWait = 180000;
   long startTime = System.currentTimeMillis();
   while (true) {
      String warning = (String)(DeltaPropagationBB.getBB().getSharedMap().get(warningKey));
      if (warning != null) {
         if (warning.indexOf("The following exception occurred while attempting to put entry") >= 0) {
            if ((warning.indexOf("Causing an error in hasDelta") >= 0) ||
                (warning.indexOf("Causing an error in toDelta") >= 0) ||
                (warning.indexOf("Causing an error in fromDelta") >= 0) ||
                (warning.indexOf("ArrayIndexOutOfBoundsException") >= 0) ||
                (warning.indexOf("IOException") >= 0) ||
                (warning.indexOf("EOFException") >= 0)) {
               Log.getLogWriter().info("Found expected warning: " + warning);
               return;
            }
         }
      }
      if (System.currentTimeMillis() - startTime > millisToWait) {
         throw new TestException("Waited " + millisToWait + "ms for a warning indicating fromDelta " +
                   "failed when invoked in client in server-to-client delta distribution");
      }
      Log.getLogWriter().info("Waiting for warning...");
      MasterController.sleepForMs(2000);
   }
}

}
