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

import hydra.Log;

import java.util.HashMap;
import java.util.HashSet;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * 
 * @author Aneesh Karayil
 * @since 5.2
 * 
 */
public class DynamicInterestEventListener extends hct.EventListener {

  static Object lock = new Object();

  public final static String OPS_KEYS = "OPS KEYS : ";

  /**
   * A local map to store the last received values for the keys in the callback
   * events.
   */
  public void afterCreate(EntryEvent event) {
    super.afterCreate(event);

    Log.getLogWriter().info("Invoking the durable Listener");
    String key = (String)event.getKey();

    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    Log.getLogWriter().info("Durable vm id is " + VmDurableId);

    synchronized (lock) {
      HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
          VmDurableId);
      if (!map.containsKey(OPS_KEYS)) {
        map.put(OPS_KEYS, new HashSet());
      }

      HashSet hashSet = (HashSet)map.get(OPS_KEYS);
      hashSet.add(key);
      map.put(OPS_KEYS, hashSet);

      DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      Log.getLogWriter().info(
          "Updated the BB for the afterCreate for the key " + key
              + " for the event ");
    }
  }

  public void afterUpdate(EntryEvent event) {
    super.afterUpdate(event);

    Log.getLogWriter().info("Invoking the durable Listener");

    String key = (String)event.getKey();
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    synchronized (lock) {
      HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
          VmDurableId);

      if (!map.containsKey(OPS_KEYS)) {
        map.put(OPS_KEYS, new HashSet());
      }

      HashSet hashSet = (HashSet)map.get(OPS_KEYS);
      hashSet.add(key);
      map.put(OPS_KEYS, hashSet);

      DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      Log.getLogWriter().info(
          "Updated the BB for the afterUpdate for the key " + key
              + " for the event ");
    }

  }

  public void afterDestroy(EntryEvent event) {
    super.afterDestroy(event);

    Log.getLogWriter().info("Invoking the durable Listener");

    String key = (String)event.getKey();
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem.getAnyInstance()).getConfig().getDurableClientId();
    
    synchronized (lock) {
       HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(VmDurableId);
       if (!map.containsKey(OPS_KEYS)) {
         map.put(OPS_KEYS, new HashSet());
       }

       HashSet hashSet = (HashSet)map.get(OPS_KEYS);
       hashSet.add(key);
       map.put(OPS_KEYS, hashSet);

       DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
       Log.getLogWriter().info("Updated the BB for the afterDestroy for the key " + key + " for the event ");
    }
  }

  public void afterInvalidate(EntryEvent event) {
    super.afterInvalidate(event);

    Log.getLogWriter().info("Invoking the durable Listener");

    String key = (String)event.getKey();
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    synchronized (lock) {
      HashMap map = (HashMap)DurableClientsBB.getBB().getSharedMap().get(VmDurableId);

      if (!map.containsKey(OPS_KEYS)) {
        map.put(OPS_KEYS, new HashSet());
      }

      HashSet hashSet = (HashSet)map.get(OPS_KEYS);
      hashSet.add(key);
      map.put(OPS_KEYS, hashSet);

      DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      Log.getLogWriter().info("Updated the BB for afterInvalidate for the key " + key + " for the event ");
    }
  }

}
