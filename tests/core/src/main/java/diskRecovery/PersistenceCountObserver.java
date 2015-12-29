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
package diskRecovery;

import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver;

import hydra.Log;

import java.util.concurrent.atomic.AtomicInteger;

/** Class to count the number of persistent observer events.
 * @author lynn
 *
 */
public class PersistenceCountObserver implements PersistenceObserver {
  
  // counters go here; if you need more, add them
  public static AtomicInteger afterPersistedOfflineCount = new AtomicInteger();

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#memberOffline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public boolean memberOffline(String regionName, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.memberOffline for region " + regionName + ", persistentID " + persistentID);
    return true; // the default
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#memberOnline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public boolean memberOnline(String regionName, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.memberOnline for region " + regionName + ", persistentID " + persistentID);
    return true; // the default
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#memberRemoved(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public boolean memberRemoved(String regionName, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.memberRemoved for region " + regionName + ", persistentID " + persistentID);
    return true; // the default
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#afterPersistedOffline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public void afterPersistedOffline(String fullPath, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.afterPersistedOffline for region " + fullPath + ", persistentID " + persistentID);
    if (!fullPath.equals("/PdxTypes")) { // don't count the pdx registry
      afterPersistedOfflineCount.incrementAndGet();
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#afterPersistedOnline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public void afterPersistedOnline(String fullPath, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.afterPersistedOnline for region " + fullPath + ", persistentID " + persistentID);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#afterRemovePersisted(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public void afterRemovePersisted(String fullPath, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.afterRemovePersisted for region " + fullPath + ", persistentID " + persistentID);
  }

}
