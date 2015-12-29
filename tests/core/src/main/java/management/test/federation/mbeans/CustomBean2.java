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

import hydra.RemoteTestModule;

import java.net.InetAddress;

import javax.management.AttributeChangeNotification;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

public class CustomBean2 extends NotificationBroadcasterSupport implements Custom2MXBean {

  private int int2 = 0;
  private double d2 = 0.0d;
  private char c2 = 'a';
  private String str2 = "str1";
  private String templateObjectName = "DefualtDomain:type=CustomMXBean2";

  @Override
  public int getInt2() {
    return int2;
  }

  @Override
  public double getDouble2() {
    return d2;
  }

  @Override
  public char getChar2() {
    return c2;
  }

  @Override
  public String getString2() {
    return str2;
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

}
