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

import javax.management.Notification;
import javax.management.ObjectName;

public class JMXNotification implements JMXEvent {

  private static final long serialVersionUID = -6294534393513533169L;
  private Notification jmxNotificaiton = null;
  private Object handback;
  private ObjectName name;

  public JMXNotification(Notification notification, Object handback, ObjectName name) {
    this.jmxNotificaiton = notification;
    this.handback = handback;
    this.name = name;
  }

  @Override
  public boolean isNotification() {
    return true;
  }

  public Notification getJmxNotificaiton() {
    return jmxNotificaiton;
  }

  @Override
  public boolean isOpertion() {
    return false;
  }

  @Override
  public ObjectName getObjectName() {
    return name;
  }
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("Recorded Notif [ Type ").append(jmxNotificaiton.getType())
    .append(" source : ").append(jmxNotificaiton.getSource())
    .append(" message : ").append(jmxNotificaiton.getMessage())
    .append(" userData : ").append(jmxNotificaiton.getUserData())
    .append(" timestamp : ").append(jmxNotificaiton.getTimeStamp())
    .append(" ]");
    return sb.toString();
  }

}
