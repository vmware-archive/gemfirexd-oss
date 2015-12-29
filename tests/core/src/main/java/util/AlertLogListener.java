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
import util.*;
import com.gemstone.gemfire.admin.*;

public class AlertLogListener implements AlertListener {

  /** Log the alert
   */
  public void alert(Alert alert) {
    String alertStr = getAlertString(alert);
    Log.getLogWriter().info(alertStr);
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
    }
    return aStr.toString();
  }

}
