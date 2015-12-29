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

public interface Custom1MXBean {

  /* Dummy */
  public int getInt1();

  public double getDouble1();

  public char getChar1();

  public String getString1();

  /* MBeanState */
  public String getTemplateObjectName();

  public void incrementCountersTogether();

  public void decrementCountersTogether();

  public void changeBwhenAisEven();

  public void mirrorCounters();

  public int getA();

  public int getB();

  /* MBeanOperations */
  public String doJmxOp();

  /* MBeanNotification */
  public void sendNotificationToMe(String name);

  /* For stale replication */
  public int getCounter();

  public void setCounter(int newValue);

  public void resetCounterToValue(int value);

  /* Add open-types */

}
