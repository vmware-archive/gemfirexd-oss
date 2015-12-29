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
package management.jmx;

import static management.util.HydraUtil.ObjectToString;
import static management.util.HydraUtil.logInfo;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import util.TestException;

public class JMXNotificationListener implements NotificationListener {

  private JMXEventRecorder recorder = null;
  private ObjectName objectName = null;
  private String filterBasedonSource = null;
  private boolean applyFilter = false;
  private String prefix = null;

  public void setFilterBasedonSource(boolean filterBasedonSource) {
    this.applyFilter = true;
  }

  public JMXNotificationListener(String prefix, JMXEventRecorder recorder, ObjectName name) {
    this.recorder = recorder;
    this.objectName = name;
    this.prefix = prefix;
  }
  
  public JMXNotificationListener(String prefix, JMXEventRecorder recorder, ObjectName name, String filter) {
    this.recorder = recorder;
    this.objectName = name;
    this.filterBasedonSource = filter;
    applyFilter = true;
    this.prefix = prefix;
  }

  public JMXNotificationListener() {
    this.recorder = JMXBlackboard.getBB();
    this.prefix = "<<UNNAMED>>";
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    try {
      boolean filterOrNot = !applyFilter || (applyFilter && filterBasedonSource.equals(notification.getSource())); 
      if(filterOrNot){
        logInfo("JMXNotificationListener(" + prefix + ") : Received JMX Notification " + notification.getType() + " from " + notification.getSource() + " for "
            + objectName);
        printJMXNotification(notification, handback);
        JMXNotification blackboardNotificaton = new JMXNotification(notification, handback, objectName);
        recorder.addEvent(blackboardNotificaton);
      }else{
        logInfo("JMXNotificationListener(" + prefix + ") : Filtered Notification " + notification.getType() + " from " + notification.getSource() + " for "
            + objectName);        
      }
    } catch (Exception e) {
      throw new TestException("Notification Listener error", e);
    }
  }

  private void printJMXNotification(Notification notification, Object handback) {
    StringBuilder sb = new StringBuilder();
    sb.append("JMXNotificationListener(" + prefix + ") : Notification [ type=").append(notification.getType()).append(", message=")
        .append(notification.getMessage())
        .append(", source=").append(notification.getSource())
        .append(", seqNo=").append(notification.getSequenceNumber())
        .append(", timestamp=").append(notification.getTimeStamp())
        .append(", data=").append(ObjectToString(notification.getUserData()))
        .append(", handbackObject=").append(ObjectToString(handback)).append(" ]");
    logInfo(sb.toString());
  }

}
