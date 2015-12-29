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
package durableClients;

import java.util.HashMap;

import util.*;
import hydra.*;

import cq.CQUtilBB;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * CQ Op Listener. Used also for event validation.
 * 
 * @author lhughes
 * @author akarayil
 * @since 5.5
 */
public class CQOpListener extends util.AbstractListener implements CqListener {

  static Object lock = new Object();

  // Implement the CqListener interface
  public void onEvent(CqEvent event) {
    DurableClientsTest.lastEventReceivedTime = System.currentTimeMillis();

    logCQEvent("onEvent", event);

    Operation op = event.getQueryOperation();

    // increment Query Operation counters
    if (op.equals(Operation.CREATE)) {

      String key = (String)event.getKey();

      if (key.equals(durableClients.Feeder.LAST_KEY)) {
        DurableClientsTest.receivedLastKey = true;
        Log.getLogWriter().info("'last_key' received at client");
      }

      Long value = (Long)event.getNewValue();

      if (value == null) {
        throwException("value in afterCreate cannot be null: key = " + key);
      }

      String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
          .getAnyInstance()).getConfig().getDurableClientId();
      synchronized (lock) {

        boolean isDuplicate = false;

        if (value.longValue() != 0) {
          isDuplicate = validateIncrementByOne(key, value);
        }

        if (!isDuplicate) {

          HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
              VmDurableId);
          if (!map.containsKey(key)) {
            Log.getLogWriter().info(
                "Putting thread map into the vm for the vm " + VmDurableId
                    + " key" + key);
            HashMap threadMap = new HashMap();
            threadMap.put("EVENT No :", new Integer(0));
            map.put(key, threadMap);
          }

          HashMap threadMap = (HashMap)map.get(key);
          int eventNo = ((Integer)threadMap.get("EVENT No :")).intValue();
          threadMap.put("EVENT No :", new Integer(++eventNo));
          threadMap.put("EVENT SR. No : " + eventNo, event.getNewValue());
          threadMap.put(key, value);
          map.put(key, threadMap);

          DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);

        }
      }
    }
    else if (op.equals(Operation.UPDATE)) {
      String key = (String)event.getKey();
      Long newValue = (Long)event.getNewValue();

      if (newValue == null) {
        throwException("newValue in afterUpdate cannot be null: key = " + key);
      }

      String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
          .getAnyInstance()).getConfig().getDurableClientId();

      synchronized (lock) {

        boolean isDurable = false;

        isDurable = validateIncrementByOne(key, newValue);

        if (!isDurable) {

          HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
              VmDurableId);
          if (!map.containsKey(key)) {
            Log.getLogWriter().info(
                "Putting thread map into the vm for the vm " + VmDurableId
                    + " key" + key);
            HashMap threadMap = new HashMap();
            threadMap.put("EVENT No :", new Integer(0));
            map.put(key, threadMap);
          }

          HashMap threadMap = (HashMap)map.get(key);
          int eventNo = ((Integer)threadMap.get("EVENT No :")).intValue();
          threadMap.put("EVENT No :", new Integer(++eventNo));
          threadMap.put("EVENT SR. No : " + eventNo, event.getNewValue());
          threadMap.put(key, newValue);
          map.put(key, threadMap);

          DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);

        }
      }
    }
  }

  public void onError(CqEvent event) {
    logCQEvent("onError", event);
  }

  public void close() {
  }

  private boolean validateIncrementByOne(String key, Long newValue) {
    boolean isDuplicate = false;
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
        VmDurableId);
    HashMap threadMap = (HashMap)map.get(key);
    if (threadMap == null) {
      Log.getLogWriter().info(
          "ThreadMap is null for vm " + VmDurableId + " for key " + key);
    }
    else {
      Long oldValue = (Long)threadMap.get(key);
      if (oldValue == null) {
        throwException("oldValue in latestValues cannot be null: key = " + key
            + " & newVal = " + newValue);
      }
      long diff = newValue.longValue() - oldValue.longValue();
      if (diff > 1) {
        throwException("difference expected in newValue and oldValue is 1 or less (duplicates allowed), but is was "
            + diff
            + " for key = "
            + key
            + " & newVal = "
            + newValue
            + "vm is "
            + VmDurableId);
      }
      if (diff < 1) {
        isDuplicate = true;
      }
    }
    return isDuplicate;
  }

  public void throwException(String reason) {

    long exceptionNumber = DurableClientsBB.getBB().getSharedCounters()
        .incrementAndRead(DurableClientsBB.NUM_EXCEPTION);

    DurableClientsBB.getBB().getSharedMap().put(new Long(exceptionNumber),
        reason);
    
    DurableClientsBB.getBB().getSharedCounters().incrementAndRead(
        DurableClientsBB.NUM_COMPLETED_EXCEPTION_LOGGING);
  }

}
