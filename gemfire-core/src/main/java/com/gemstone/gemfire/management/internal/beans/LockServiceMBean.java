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
package com.gemstone.gemfire.management.internal.beans;

import java.util.Map;

import javax.management.NotificationBroadcasterSupport;

import com.gemstone.gemfire.management.LockServiceMXBean;

/**
 * Management API to manage a Lock Service MBean
 * 
 * @author rishim
 * 
 */
public class LockServiceMBean extends NotificationBroadcasterSupport implements
    LockServiceMXBean {

  private LockServiceMBeanBridge bridge;

  public LockServiceMBean(LockServiceMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public void becomeLockGrantor() {
    bridge.becomeLockGrantor();

  }

  @Override
  public String fetchGrantorMember() {
    return bridge.fetchGrantorMember();
  }

  
  @Override
  public int getMemberCount() {

    return bridge.getMemberCount();
  }

  @Override
  public String[] getMemberNames() {

    return bridge.getMemberNames();
  }

  @Override
  public String getName() {

    return bridge.getName();
  }

  @Override
  public Map<String, String> listThreadsHoldingLock() {

    return bridge.listThreadsHoldingLock();
  }

  @Override
  public boolean isDistributed() {

    return bridge.isDistributed();
  }

  @Override
  public boolean isLockGrantor() {

    return bridge.isLockGrantor();
  }

  @Override
  public String[] listHeldLocks() {

    return bridge.listHeldLocks();
  }

}
