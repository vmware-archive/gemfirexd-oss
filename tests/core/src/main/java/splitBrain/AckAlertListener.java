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

public class AckAlertListener implements AlertListener {

public void alert(Alert alert) {
   try {
      Log.getLogWriter().info(getAlertString(alert));
      detectDesiredAlert(alert); 
   } catch (Exception e) {
      String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + " " + TestHelper.getStackTrace(e);
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
   }
}

/** Detect if we got a desired alert in any vm. A desired alert is
 *  one where the alert is severe, and shows we have waited an
 *  appropriate amount of time, based on test settings. 
 */  
private void detectDesiredAlert(Alert alert) {
   if (alert.getLevel().equals(AlertLevel.SEVERE)) { // alert is severe
      String msg = alert.getMessage();
      String waitingForRepliesStr = " seconds have elapsed while waiting for replies";
      String waitingForResponseStr = " seconds have elapsed waiting for a response from ";
      String unableToFormConnStr = "Unable to form a TCP/IP connection to ";
      if (msg.indexOf(waitingForRepliesStr) >= 0) { // alert message is waiting for replies
         processWaitingForReplies(alert, waitingForRepliesStr);
      } else if (msg.indexOf(waitingForResponseStr) >= 0) { // alert message is waiting for response
         processWaitingForResponse(alert, waitingForResponseStr);
      } else if (msg.indexOf(unableToFormConnStr) >= 0) { // alert message is unable to form connection
         processUnableToFormConn(alert, unableToFormConnStr);
      }
   }
}

/** Process a severe waiting for replies message
 *  
 *  @param alert The severe alert that was raised.
 *  @param waitingForRepliesStr The waiting for replies text contained in the message.
 *  
 */
private void processWaitingForReplies(Alert alert, String waitingForRepliesStr) {
   String msg = alert.getMessage();
   int index = msg.indexOf(waitingForRepliesStr);
   String seconds = msg.substring(0, index);
   validateSevereAlert(alert, seconds);
}

/** Process a severe waiting for response message
 *
 *  @param alert The severe alert that was raised.
 *  @param waitingForResponseStr The waiting for reponse text contained in the message.
 */
private void processWaitingForResponse(Alert alert, String waitingForResponseStr) {
   String msg = alert.getMessage();
   int index = msg.indexOf(waitingForResponseStr);
   String seconds = msg.substring(0, index);
   String whiteSpaceDelim = "[\\s]+"; // separates white space
   String[] tokenArr = seconds.split(whiteSpaceDelim); 
   seconds = tokenArr[tokenArr.length-1];
   validateSevereAlert(alert, seconds);
}

/** Process a severe unable to form connection message.
 *
 *  @param alert The severe alert that was raised.
 *  @param unableToFormConnStr The unable to form connectin text contained in the message.
 */
private void processUnableToFormConn(Alert alert, String unableToFormConnStr) {
   String msg = alert.getMessage();
   String beforeSecondsStr = " in over ";
   String afterSecondsStr = " seconds";
   int index1 = msg.indexOf(beforeSecondsStr) + beforeSecondsStr.length();
   int index2 = msg.indexOf(afterSecondsStr);
   String seconds = msg.substring(index1, index2);
   validateSevereAlert(alert, seconds);
}

/** Given a severe alert message, and the number of seconds it specifies it
 *  waited, validate this alert.
 * 
 *  @param alert The severe alert that was raised. Its message is already 
 *         known to be an acceptable flavor to the test (meaning it is one 
 *         of "waiting for replies", "waiting for response", or "unable to 
 *         form TCP/IP connection".
 *  @param seconds A string taken out of the msg which is the number of seconds
 *         the message says it waited.                   
 */
protected void validateSevereAlert(Alert alert, String seconds) {
   String msg = alert.getMessage();
   int msgInt = 0;
   try {
      msgInt = (Integer.valueOf(seconds)).intValue();
   } catch (NumberFormatException e) {
      // not expected; there should be an int 
      String errStr = "Test expected " + msg + " to have a different format";
      Log.getLogWriter().info(errStr);
      ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
      return;
   }

   // get the pid being targeted in the alert message
   int index1 = msg.indexOf("(");
   int index2 = msg.indexOf(")");
   String msgPID = msg.substring(index1+1, index2);
   index1 = msgPID.indexOf(":");
   msgPID = msgPID.substring(index1+1);
   final int msgVmid = ControllerBB.getVmIdForPid(Integer.valueOf(msgPID).intValue());
   
   // Check to see if the pid being targeted in the alert message is expecting to be the target 
   // of an alert
   if (ControllerBB.isAlertEnabled(msgVmid)) {
      String errStr = checkWaitTime(msgInt, msg);
      if (errStr != null) {
         Log.getLogWriter().info(errStr);
         ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
         return;
      } 
      ControllerBB.signalSevereAlert(msgVmid);
      Log.getLogWriter().info("Found alert to target vm " + msgVmid + ": " + getAlertString(alert));
   } else { // the pid targeted in the alert message was not expecting to be the target of an alert
      /* sometimes alerts are waiting for healthy vms; this is why we changed the product
         from causing forced disconnects to issuing alerts; for now we need to allow alerts
         that are waiting for healthy vms
      */
      Log.getLogWriter().info("This alert is targeting a healthy vm " + msgVmid + ": " + getAlertString(alert));
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
   aStr.append("   alert.getSystemMember(): " + alert.getSystemMember());
   return aStr.toString();
}

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
   if (waitTimeInMessage < sum) { // check if wait is long enough to honor ackWaitThreshold and ackSevereAlertThreshold
      String errStr = "ackWaitThreshold is " + ackWaitThreshold + ", ackSevereAlertThreshold is " +
         ackSevereAlertThreshold + ", sum is " + sum + ", expected " + msg + " to wait at least " +
         sum + " seconds";
      return errStr;
   } 
   return null;
}

}
