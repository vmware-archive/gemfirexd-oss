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

import javax.management.NotificationBroadcaster;

public interface Custom2MXBean {
  public int getInt2();

  public double getDouble2();

  public char getChar2();

  public String getString2();

  public String getTemplateObjectName();

  public void incrementCountersTogether();

  public void decrementCountersTogether();

  public void changeBwhenAisEven();

  public void mirrorCounters();

  public int getA();

  public int getB();

  public String doJmxOp();

  public void sendNotificationToMe(String name);
}
