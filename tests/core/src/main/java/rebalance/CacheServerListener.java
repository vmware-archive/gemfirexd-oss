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
package rebalance; 

import hydra.FileUtil;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.MasterProxyIF;
import hydra.ProcessMgr;
import hydra.RmiRegistryHelper;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;

/** 
 * CacheServerListener installs a ResourceObserver (statically).
 * For use in tests which invoke cacheserver script (so we can track
 * rebalancing progress).
 *
 * @author lhughes
 * @since 6.0
 */
public class CacheServerListener extends CacheListenerAdapter implements Declarable {

/**  Stub reference to a remote master controller */
public static MasterProxyIF Master = null;
protected static LogWriter log = null;

static {

    // initialization in CacheServer VM
    // Open a LogWriter 
    try {
       Log.getLogWriter();
    } catch (HydraRuntimeException e) {
       // create the task log
       String logName = "cacheserver_" + ProcessMgr.getProcessId();
       log = Log.createLogWriter( logName, "info" );
   }

   // We need to set user.dir (for access to TestConfig)
   // RMI lookup of Master (for Blackboard access), relies on TestConfig
   String cacheServerDir = System.getProperty("user.dir");
   String testDir = FileUtil.pathFor(cacheServerDir);
   System.setProperty("test.dir", testDir);
   
   if ( Master == null )
      Master = RmiRegistryHelper.lookupMaster();

   Log.getLogWriter().info("CacheServerListener installing ResourceObserver");
   InternalResourceManager.setResourceObserver(
      new ResourceObserverAdapter() {

         public void rebalancingOrRecoveryStarted(Region aRegion) {
            Log.getLogWriter().info("CacheServerListener ResourceObserver.rebalanceStarted() for Region " + aRegion.getName());

         }

         public void rebalancingOrRecoveryFinished(Region aRegion) {
            Log.getLogWriter().info("CacheServerListener ResourceObserver.rebalanceFinished() for Region " + aRegion.getName());
            RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.recoveryRegionCount);
         }
      });
      Log.getLogWriter().info("CacheServerListener installed ResourceObserver");
   }

   public void init(java.util.Properties prop) {
      // do nothing
   }
}
