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
import hydra.RemoteTestModule;

/** Class to help create a "tie" to test how GemFire chooses the most recent
 *   disk files to create. 
 * @author lynn
 *
 */
public class RecoveryTestObserver implements PersistenceObserver {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#memberOffline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public boolean memberOffline(String regionName, PersistentMemberID persistentID) {
    return allowPersistedMemberInfo(regionName, persistentID, "memberOffline");
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#memberOnline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public boolean memberOnline(String regionName, PersistentMemberID persistentID) {
    return allowPersistedMemberInfo(regionName, persistentID, "memberOnline");
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#memberRemoved(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public boolean memberRemoved(String regionName, PersistentMemberID persistentID) {
    return allowPersistedMemberInfo(regionName, persistentID, "memberRemoved");
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver#afterPersistedOffline(java.lang.String, com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID)
   */
  public void afterPersistedOffline(String fullPath, PersistentMemberID persistentID) {
    Log.getLogWriter().info("In RecoveryPersistObserver.afterPersistedOffline for region " + fullPath + ", persistentID " + persistentID);
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

  /** Return true if the product should persist membership information into disk files
   *   as is the default behavior, false if the test wants to modify the behavior to
   *   create a "tie" during recovery. 
   * @param regionName Region name from test hook invocation
   * @param persistentID persistentID from test hook invocation
   * @param hookName the method name of the hook that was called (for logging purposes)
   * @return true if the product should persiste membership information, false otherwise
   */
  private boolean allowPersistedMemberInfo(String regionName, PersistentMemberID persistentID, String hookName) {
    Object key = RecoveryBB.allowPersistedMemberInfo + RemoteTestModule.getMyVmid();
    Boolean allowPersistedMemberInfo = (Boolean)(RecoveryBB.getBB().getSharedMap().get(key));
    if (allowPersistedMemberInfo == null) {
      allowPersistedMemberInfo = true; // the default
    }
    Log.getLogWriter().info("In RecoveryPersistObserver." + hookName + " for region " + regionName + ", persistentID " + persistentID +
        ", returning " + allowPersistedMemberInfo);
    return allowPersistedMemberInfo;
  }

}
