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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

public class Expectation implements Serializable {

  private static final long serialVersionUID = -7950183141936995134L;

  private ObjectName objectName;
  private String url;
  private List<ExpectedNotification> notifications = new ArrayList<ExpectedNotification>();
  private Map<String, Object> attributes = new HashMap<String, Object>();
  private Map<String, Object> operations = new HashMap<String, Object>();
  
  public static final int MATCH_NOTIFICATION_SOURCE_AND_MESSAGE=0;
  public static final int MATCH_NOTIFICATION_MESSAGE=1;
  public static final int MATCH_NOTIFICATION_MESSAGECONTAINS_COUNT=5;
  public static final int MATCH_NOTIFICATION_SOURCE_AND_USERDATA=2;
  public static final int MATCH_NOTIFICATION_SOURCE_AND_MESSAGECONTAINS=4;

  public static class ExpectedNotification {
    public long timestamp;
    public Object userData;
    public Object source;
    public String type;
    public String message;
    public int matchMode;
    public int matchCount=-1;
        
    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append("Expected Notif [ Type ").append(type)
      .append(" source : ").append(source)
      .append(" message : ").append(message)
      .append(" matchCount : ").append(matchCount)
      //.append(" matchMode : ").append(toMatchModeString())
      .append(" userData : ").append(userData)
      .append(" timestamp : ").append(timestamp)
      .append(" ]");
      return sb.toString();
    }
    
  }

  public Expectation(ObjectName mbean) {
    this.objectName = mbean;
  }
  
  public Expectation expectNotification(String notificationType, Object source,String message, int matchCount,
      int mode) {
    ExpectedNotification e = new ExpectedNotification();
    e.timestamp = System.currentTimeMillis();
    e.source = source;
    e.userData = null;
    e.matchCount = matchCount;
    e.type = notificationType;
    e.message = message;
    e.matchMode = mode;
    this.notifications.add(e);
    
    // just to be on safer side sort using timestamp so that validation happens
    // in correct order
    Collections.sort(this.notifications, new Comparator<ExpectedNotification>() {
      @Override
      public int compare(ExpectedNotification o1, ExpectedNotification o2) {
        return (int) (o1.timestamp - o2.timestamp);
      }
    });

    return this;
  }

  /**
   * Make sure you add notification to expectation in order expected so that
   * validation happens in correct order.
   * 
   * @param notificationType
   * @param userData
   * @param source
   * @return
   */
  public Expectation expectNotification(String notificationType, Object source,String message, Object userData,
      int mode) {
    ExpectedNotification e = new ExpectedNotification();
    e.timestamp = System.currentTimeMillis();
    e.source = source;
    e.userData = userData;
    e.type = notificationType;
    e.message = message;
    e.matchMode = mode;
    this.notifications.add(e);
    
    // just to be on safer side sort using timestamp so that validation happens
    // in correct order
    Collections.sort(this.notifications, new Comparator<ExpectedNotification>() {
      @Override
      public int compare(ExpectedNotification o1, ExpectedNotification o2) {
        return (int) (o1.timestamp - o2.timestamp);
      }
    });

    return this;
  }

  public Expectation expectMBeanAt(String url) {
    this.url = url;
    return this;
  }

  public Expectation expectAttribute(String attribute, Object value) {
    attributes.put(attribute, value);
    return this;
  }

  public Expectation expectOperation(String operationName, Object result) {
    operations.put(operationName, result);
    return this;
  }

  public String getUrl() {
    return url;
  }

  public List<ExpectedNotification> getNotifications() {
    return notifications;
  }

  public Map<String, Object> getAttributes() {
    return attributes;
  }

  public Map<String, Object> getOperations() {
    return operations;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (objectName != null)
      sb.append("[ ON : " + objectName.toString());
    sb.append(" URL : ").append(url);
    sb.append("\n notifications : ").append(notifications);
    sb.append(" attributes : ").append(attributes.keySet());
    sb.append(" operations : ").append(operations.keySet());
    sb.append(" ]");
    return sb.toString();
  }

  public ObjectName getObjectName() {
    return objectName;
  }

  public ObjectName getObjectName(String url2) {
    return objectName;
  }

}
