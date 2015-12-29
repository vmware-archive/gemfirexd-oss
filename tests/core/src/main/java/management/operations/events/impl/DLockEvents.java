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
package management.operations.events.impl;

import static management.util.HydraUtil.logInfo;

import java.util.List;
import java.util.Map;

import management.operations.OperationEvent;
import management.operations.OperationPrms;
import management.operations.events.DLockOperationEvents;

import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class DLockEvents extends AbstractEvents implements DLockOperationEvents {

  public DLockEvents(Map<String, List<OperationEvent>> eventMap) {
    super(eventMap);
    // TODO Auto-generated constructor stub
  }

  public DLockEvents() {
  }

  @Override
  public void dlockCreated(String name, DistributedLockService service) {
    if (OperationPrms.recordDlockOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_DLOCK_ADDED;
      Object array[] = { name, System.currentTimeMillis() }; // store name and time
      event.data = array;
      addEvent(event);
    }
    logInfo("DLockOperations: Finished with creating DLock : " + name);
  }

  @Override
  public void dlockDestroyed(String name, DistributedLockService service) {
    if (OperationPrms.recordDlockOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_DLOCK_DESTROYED;
      Object array[] = { name, System.currentTimeMillis() }; // store name and time
      event.data = array;
      addEvent(event);
    }
    logInfo("DLockOperations: Finished with destroying DLock : " + name);
  }

  @Override
  public void dlockLockUnLock(String name, DistributedLockService service, long l1, long l2, String lockName) {
    if (OperationPrms.recordDlockOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_DLOCK_LOCKUNLOCK;
      Object array[] = { name, l1, l2, lockName}; // store name and time
      event.data = array;
      addEvent(event);
    }
    logInfo("DLockOperations: Finished with lockunlocking DLock : " + name);
  }

  @Override
  public void dlockBecomeGrantor(String name, DistributedLockService service) {
    if (OperationPrms.recordDlockOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_DLOCK_BECOMEGRANTOR; //
      String memberId = InternalDistributedSystem.getConnectedInstance().getDistributedMember().getId();
      Object array[] = { name, memberId }; // store name and memberName
      event.data = array;
      addEvent(event);
    }
    logInfo("DLockOperations: Finished with lockunlocking DLock : " + name);
  }

}
