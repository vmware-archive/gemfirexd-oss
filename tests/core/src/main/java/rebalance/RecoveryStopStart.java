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

import hydra.*;
import hydra.blackboard.*;

import util.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

import parReg.*;

public class RecoveryStopStart {

/** Hydra task to stop/start a single DataStore VM.
 *  This method selects from the list of dataStores on the BB and stops
 *  one dataStore.  It then waits for the remaining dataStore to host all 
 *  primaries before starting the 2nd dataStore.
 */
public static void HydraTask_stopStartDataStoreVm() {
   List vmList = getDataStoreVms();
   List stopModes = new ArrayList();
   int randInt = TestConfig.tab().getRandGen().nextInt(0, vmList.size()-1);
   ClientVmInfo targetVm = (ClientVmInfo)vmList.get(randInt);
   String stopModeStr = TestConfig.tab().stringAt(ParRegPrms.stopModes);

   // reset recovery status
   SharedCounters counters = RebalanceBB.getBB().getSharedCounters();
   Log.getLogWriter().info("clearing recoveryRegionCount prior to stop of " + targetVm);
   counters.zero(RebalanceBB.recoveryRegionCount);
   Log.getLogWriter().info("cleared recoveryRegionCount prior to stop of " + targetVm);

   // stop the target VM
   try {
      ClientVmMgr.stop( "Test is synchronously stopping " + targetVm + " with " + stopModeStr,
                        ClientVmMgr.toStopMode(stopModeStr),
                        ClientVmMgr.ON_DEMAND,
                        targetVm);
   } catch (hydra.ClientVmNotFoundException e) {
      Log.getLogWriter().info("Caught Exception " + e + " while stopping DataStoreVm " + targetVm);
   }

   // wait for remaining VM to reassign all primaries to itself (recovery complete)
   RebalanceUtil.waitForRecovery();

   // initialize the RebalanceBB counter 
   Log.getLogWriter().info("clearing recoveryRegionCount after stop and before start of " + targetVm);
   counters.zero(RebalanceBB.recoveryRegionCount);
   Log.getLogWriter().info("cleared recoveryRegionCount after stop of and before start of " + targetVm);

   // restart the stopped VM
   try {
      ClientVmMgr.start("Test is synchronously starting " + targetVm, targetVm);
   } catch (hydra.ClientVmNotFoundException e) {
      Log.getLogWriter().info("Caught Exception " + e + " while starting DataStoreVm " + targetVm);
   }

   Log.getLogWriter().info("Done in RecoveryStopStart.stopStartDataStoreVm()");
}

/** Return a list of the ClientVmInfo for all data store VMs. These are obtained 
 *  from the blackboard map. 
 */
public static String DataStoreVmStr = "DataStoreVM_";
protected static List getDataStoreVms() {
   List aList = new ArrayList();
   Map aMap = ParRegBB.getBB().getSharedMap().getMap();
   Iterator it = aMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      if (key instanceof String) {
         if (((String)key).startsWith(DataStoreVmStr)) {
            aList.add(new ClientVmInfo((Integer)(aMap.get(key)), null, null));
         }
      }
   }
   return aList;
}

}
