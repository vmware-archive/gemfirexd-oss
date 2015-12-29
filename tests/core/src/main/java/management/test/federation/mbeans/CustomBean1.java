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
package management.test.federation.mbeans;

import javax.management.AttributeChangeNotification;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import hydra.RemoteTestModule;

public class CustomBean1 extends NotificationBroadcasterSupport implements Custom1MXBean {

  private int int1 = 0;
  private double d1 = 0.0d;
  private char c1 = 'a';
  private String str1 = "str1";
  private String templateObjectName = "DefualtDomain:type=CustomMXBean1";

  @Override
  public int getInt1() {
    return int1;
  }

  @Override
  public double getDouble1() {
    return d1;
  }

  @Override
  public char getChar1() {
    return c1;
  }

  @Override
  public String getString1() {
    return str1;
  }

  @Override
  public String getTemplateObjectName() {
    return templateObjectName;
  }

  private int a = 0, b = 0;

  @Override
  public void incrementCountersTogether() {
    a++;
    b++;
  }

  @Override
  public void decrementCountersTogether() {
    a--;
    b--;
  }

  @Override
  public void changeBwhenAisEven() {
    if ((a % 2) == 0) {
      b++;
    } else
      b--;
  }

  @Override
  public void mirrorCounters() {
    a++;
    b--;
  }

  @Override
  public int getA() {
    return a;
  }

  @Override
  public int getB() {
    return b;
  }

  private long sequenceNumber = 1;

  public void sendNotificationToMe(String name) {
    long sequence = sequenceNumber++;
    Notification n = new AttributeChangeNotification(this, sequenceNumber, System.currentTimeMillis(),
        name, "A", "long", sequence, sequenceNumber);
    n.setSource(RemoteTestModule.getMyVmid());
    sendNotification(n);
  }

  @Override
  public String doJmxOp() {
    String vmId = "vmId" + RemoteTestModule.getMyVmid();
    return vmId;
  }

  private int counter = 0;

  @Override
  public int getCounter() {
    return counter;
  }

  @Override
  public void setCounter(int newValue) {
    this.counter = newValue;
  }

  @Override
  public void resetCounterToValue(int value) {
    this.counter = value;
    /*
     * long sequence = sequenceNumber++;
     * Notification n = new AttributeChangeNotification(this, sequenceNumber,
     * System.currentTimeMillis(), "staticField changed", "counter",
     * "int", sequence, sequenceNumber);
     * n.setSource(RemoteTestModule.getMyPid());
     * n.setUserData(null);
     * sendNotification(n);
     */
  }

}