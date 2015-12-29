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
package management.jmx.validation;

import static management.util.HydraUtil.logFine;
import static management.util.HydraUtil.logInfo;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import management.jmx.Expectation;
import management.jmx.Expectation.ExpectedNotification;
import management.jmx.JMXBlackboard;
import management.jmx.JMXEvent;
import management.jmx.JMXEventRecorder;
import management.jmx.JMXNotification;
import management.jmx.JMXOperation;
import management.operations.ops.JMXOperations;
import management.util.HydraUtil;
import management.util.ManagementUtil;

/**
 * 
 * Validator class to verify JMX States.
 * 
 * Validation performed here is generic and checked against expected state
 * stored in the blackboard.
 * 
 * @author tushark
 * 
 */
public class JMXValidator implements Serializable {

  List<JMXEvent> events = null;
  List<Expectation> expectedEvents = null;

  public JMXValidator(JMXBlackboard blackboard) {
    this.events = blackboard.getRecordedEvents();
    this.expectedEvents = blackboard.getExpectedState();
  }

  public JMXValidator(JMXEventRecorder recorder, List<Expectation> expectedEvents) {
    this.events = recorder.getRecordedEvents();
    this.expectedEvents = expectedEvents;
  }

  /**
   * Matches recorded events with expected events Returned list contains only
   * error or unexpected Events so if list is empty that means test has passed
   * successfully.
   * 
   * @return
   */
  public List<ValidationResult> validate() {
    List<ValidationResult> list = new ArrayList<ValidationResult>();
    for (Expectation expectation : expectedEvents) {
      List<JMXEvent> eventsForThisMbean = getEventForThisMbean(events, expectation.getObjectName());
      List<ValidationResult> rsult = isExpectationSatisfied(expectation, eventsForThisMbean);
      if (rsult != null)
        list.addAll(rsult);
    }
    return list;
  }

  private List<JMXEvent> getEventForThisMbean(List<JMXEvent> events, ObjectName name) {
    List<JMXEvent> eventList = new ArrayList<JMXEvent>();
    logInfo("Filtering out events for mbean objectName " + name);
    for (JMXEvent event : events) {
      if (name.equals(event.getObjectName()))
        eventList.add(event);
    }
    logInfo("Filtered " + eventList.size() + " events out of " + events.size());
    return eventList;
  }

  private List<ValidationResult> isExpectationSatisfied(Expectation expectation, List<JMXEvent> events) {

    List<ValidationResult> list = new ArrayList<ValidationResult>();

    logInfo("Validating expectation " + expectation);

    /*-
    if(events.size() ==0){
      logFine("No events recorded for this mbean : " + expectation.getObjectName());
      list.add(ValidationResult.error(expectation.getObjectName(), null, "No events recorded for this mbean"));
      return list;
    }*/

    String url = expectation.getUrl();
    if (url == null) {
      list.add(ValidationResult.error(expectation.getObjectName(), null, "NO urls specified for the targeted mbean"));
      logFine("Validation failed url not reachable " + url);
      return list;
    }

    MBeanServerConnection remoteMBS = null;
    ObjectName name = expectation.getObjectName();

    try {
      if(!JMXOperations.LOCAL_MBEAN_SERVER.equals(url))
        remoteMBS = ManagementUtil.connectToUrlOrGemfireProxy(url);
      else
        remoteMBS = ManagementUtil.getPlatformMBeanServerDW();      
    } catch (MalformedURLException e) {
      list.add(ValidationResult.jmxError(name, url, e));
      return list;
    } catch (IOException e) {
      list.add(ValidationResult.jmxError(name, url, e));
      return list;
    }

    List<JMXOperation> recordedOpList = new ArrayList<JMXOperation>();
    List<JMXNotification> recordedNotificationList = new ArrayList<JMXNotification>();

    for (JMXEvent e : events) {
      if (e.isNotification())
        recordedNotificationList.add((JMXNotification) e);
      if (e.isOpertion())
        recordedOpList.add((JMXOperation) e);
    }

    // Check for attributes. return of any errors
    if (validateAttributes(expectation, list, remoteMBS))
      return list;
    // Check for operations
    validateOperations(expectation, list, recordedOpList);
    validateNotifications(expectation, list, recordedNotificationList);
    return list;
  }

  public boolean validateAttributes(Expectation expectation, List<ValidationResult> list,
      MBeanServerConnection remoteMBS) {
    String url = expectation.getUrl();
    ObjectName name = expectation.getObjectName();
    Map<String, Object> attributemap = expectation.getAttributes();

    Map<String, Object> actualAttributemap = new HashMap<String, Object>();
    for (String attr : attributemap.keySet()) {
      Object attrValue = null;
      try {
        attrValue = remoteMBS.getAttribute(expectation.getObjectName(url), attr);
      } catch (AttributeNotFoundException e1) {
        list.add(ValidationResult.jmxError(name, url, e1));
        return true;
      } catch (InstanceNotFoundException e1) {
        list.add(ValidationResult.jmxError(name, url, e1));
        return true;
      } catch (MBeanException e1) {
        list.add(ValidationResult.jmxError(name, url, e1));
        return true;
      } catch (ReflectionException e1) {
        list.add(ValidationResult.jmxError(name, url, e1));
        return true;
      } catch (IOException e1) {
        list.add(ValidationResult.jmxError(name, url, e1));
        return true;
      }
      Object expectedValue = attributemap.get(attr);
      if (expectedValue != null && !expectedValue.equals(attrValue)) {
        list.add(ValidationResult.attributeDontMatch(name, url, attr, attrValue, expectedValue));
      } else if (expectedValue != null && expectedValue.equals(attrValue)) {
        logInfo("Attribute value for attribute " + attr + " matched ");
      }
    }
    return false;
  }

  public void validateOperations(Expectation expectation, List<ValidationResult> list, List<JMXOperation> recordedOpList) {
    ObjectName name = expectation.getObjectName();
    Map<String, Object> expectedOperationMap = expectation.getOperations();
    if (recordedOpList.size() < expectedOperationMap.size()) {
      list.add(ValidationResult.error(
          name,
          null,
          "Some operations are missing from events against the expected operations list recorded="
              + recordedOpList.size() + " Expected=" + expectedOperationMap.size()));
      for (String op : expectedOperationMap.keySet()) {
        boolean flag = false;
        for (JMXOperation jmxOp : recordedOpList) {
          if (jmxOp.getOperationName().equals(op)) {
            flag = true;
            Object expected = expectedOperationMap.get(op);
            Object recorded = jmxOp.getResult();
            if (jmxOp.isError()) {
              list.add(ValidationResult.operationResultedInError(name, jmxOp.getSource(), jmxOp.getOperationName(),
                  jmxOp.getError()));
            } else {
              if (expected != null && !expected.equals(recorded)) {
                list.add(ValidationResult.operationResultUnExpected(name, jmxOp.getSource(), jmxOp.getOperationName(),
                    recorded, expected));
              }
            }
            break;
          }
        }
        // not found means error
        if (!flag) {
          list.add(ValidationResult.error(name, null, "Operation missing " + op));
        }
      }

    } else {
      for (JMXOperation jmxOp : recordedOpList) {
        Object expected = expectedOperationMap.get(jmxOp.getOperationName());
        Object object = jmxOp.getResult();
        if (jmxOp.isError()) {
          list.add(ValidationResult.operationResultedInError(name, jmxOp.getSource(), jmxOp.getOperationName(),
              jmxOp.getError()));
        } else {
          if (expected != null && !expected.equals(object)) {
            list.add(ValidationResult.operationResultUnExpected(name, jmxOp.getSource(), jmxOp.getOperationName(),
                object, expected));
          }
        }
      }
    }
  }

  /*- 
  Check for notifications : Peek at notification and check for its contents
  Validation here is very crude one. Type, UserData and Source are matched
  but in not in AND Condition If have multiple notifications it might 
  happen that validation succeeds because it matched source from one 
  location and userData from another */

  public void validateNotifications(Expectation expectation, List<ValidationResult> list,
      List<JMXNotification> recordedNotificationList) {
    ObjectName name = expectation.getObjectName();
    String url = expectation.getUrl();
    List<ExpectedNotification> expectedNotifications = expectation.getNotifications();
    if (recordedNotificationList.size() < expectedNotifications.size()) {
      list.add(ValidationResult.error(name, null, "Notifications missing recorded=" + recordedNotificationList.size()
          + " expected=" + expectedNotifications.size()));      
    }

    for (ExpectedNotification notification : expectedNotifications) {
      boolean flag = false;
      /*-
      boolean checkUserData = (notification.userData != null);
      boolean checkSource = (notification.source != null);
      boolean checkMessage = (notification.message != null);
      boolean checkUserData = notification.checkUserData;
      boolean checkSource = notification.checkSource;
      boolean checkMessage = notification.checkMessage;      
      
      boolean userDataFlag = false;
      boolean sourceFlag = false;
      boolean messageFlag = false;*/
      JMXNotification jmxn = null;
      String errorMessage = "";
      int matchCount = 0;

      for (JMXNotification n : recordedNotificationList) {
        flag=false;
        /*-
        userDataFlag = true;
        sourceFlag = true;
        messageFlag = true;*/
        
        jmxn = n;
        StringBuilder sb = new StringBuilder();
        sb.append(" validatation : " ).append(notification).append(" against ").append(n);
        if (n.getJmxNotificaiton().getType().equals(notification.type)) {
          flag = true;
          boolean modeMatch = false;
          /*-
          if (checkMessage && !n.getJmxNotificaiton().getMessage().equals(notification.message))
            messageFlag = false;
          if (checkUserData && !n.getJmxNotificaiton().getUserData().equals(notification.userData))
            userDataFlag = false;
          if (checkSource && !n.getJmxNotificaiton().getSource().equals(notification.source))
            sourceFlag = false;
          
          logFine("Validation Result Message : E=" + checkMessage + " R=" + messageFlag 
              + " UserData : E=" + checkUserData + " R=" + userDataFlag
              + " Source : E=" + checkSource + " R=" + sourceFlag);*/
          switch(notification.matchMode){
            case Expectation.MATCH_NOTIFICATION_MESSAGECONTAINS_COUNT :
              if (n.getJmxNotificaiton().getMessage().contains(notification.message))
                modeMatch = true;
              break;
            case Expectation.MATCH_NOTIFICATION_MESSAGE :
              if (n.getJmxNotificaiton().getMessage().equals(notification.message))
                modeMatch = true;
              break;
            case Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGE:
              if (n.getJmxNotificaiton().getMessage().equals(notification.message) &&
                  n.getJmxNotificaiton().getSource().equals(notification.source))
                modeMatch = true;
              break;
            case Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS:
              if ( n.getJmxNotificaiton().getMessage().contains(notification.message) &&
                  n.getJmxNotificaiton().getSource().equals(notification.source))
                modeMatch = true;
              break;
            case Expectation.MATCH_NOTIFICATION_SOURCE_AND_USERDATA :
              if (n.getJmxNotificaiton().getSource().equals(notification.source) &&
                  n.getJmxNotificaiton().getUserData().equals(notification.userData))
                modeMatch = true;
              break;
            default : 
              modeMatch = false;
              break;
          }
          
          if(flag && modeMatch){
            logFine("Validation Passed "  +sb.toString());
            flag = true;
          }
          else{
            logFine("Validation Failed." + sb.toString());
            flag =false;
          }
          
        }

        //Increase count only when type and one of property is matched
        if (flag 
            /*- && ((checkMessage && messageFlag) && 
            (checkUserData && userDataFlag) && 
            (checkSource && sourceFlag))*/){
          matchCount++;
          /*-logFine("Validation Passed - N=" + matchCount 
              + " MessageMatched " + messageFlag
              + " userDataMatched " + userDataFlag
              + " sourceMatched " + sourceFlag);*/
        }
        
      }      
      
      if (jmxn != null && matchCount==0) {        
        switch(notification.matchMode){
        case Expectation.MATCH_NOTIFICATION_MESSAGECONTAINS_COUNT :
          errorMessage += " Notification messageContains mismatch, expected message=" + HydraUtil.ObjectToString(notification.message);
          errorMessage += " Notification matchCount mismatch, expected matchCount=" + HydraUtil.ObjectToString(notification.matchCount);
          break;
        case Expectation.MATCH_NOTIFICATION_MESSAGE :
          errorMessage += " Notification message mismatch, expected message=" + HydraUtil.ObjectToString(notification.message);
          break;
        case Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS:
          errorMessage = "Notification source mismatch, expected source={" + HydraUtil.ObjectToString(notification.source) + "})";
          errorMessage += " Notification messageContains mismatch, expected message=" + HydraUtil.ObjectToString(notification.message);
          break;
        case Expectation.MATCH_NOTIFICATION_SOURCE_AND_MESSAGE:
          errorMessage = "Notification source mismatch, expected source={" + HydraUtil.ObjectToString(notification.source) + "})";
          errorMessage += " Notification message mismatch, expected message=" + HydraUtil.ObjectToString(notification.message);
          break;
        case Expectation.MATCH_NOTIFICATION_SOURCE_AND_USERDATA :
          errorMessage = "Notification source mismatch, expected source={" + HydraUtil.ObjectToString(notification.source) + "})";
          errorMessage += " Notification userData mismatch, expected data=" + HydraUtil.ObjectToString(notification.userData);
          break;
        default : 
          errorMessage = "Unknown mode of notification matching";
          break;
        }
        list.add(ValidationResult.missingNotification(name, url, notification.type, errorMessage));        
        
      }else if(matchCount > 1 && notification.matchCount!=-1){
        if(matchCount==notification.matchCount){
          logFine("Validation Passed for match count "  +notification.matchCount);
        }else{
          list.add(ValidationResult.missingNotification(name, url, notification.type, 
              "Match count mismatch expected=" + notification.matchCount + " recorded=" + matchCount));
        }
      }
      else if (matchCount > 1 && notification.matchCount==-1) {
        list.add(ValidationResult.error(name, null, "More than one Notification(Count=" + matchCount +") (Probably #45748) for "+notification));
      }
    }
  }

}
