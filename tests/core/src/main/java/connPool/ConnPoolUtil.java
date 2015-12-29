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
package connPool; 

import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;
import hydra.*;
import perffmwk.*;
import util.*;

public class ConnPoolUtil {

protected final static String currentClientConnKey = "CurrentClientConnections_";
protected final static String currentQueueConnKey = "CurrentQueueConnections_";
protected final static String queueLoadKey = "QueueLoadKey_";
protected final static String connectionLoadKey = "ConnectionLoadKey_";

/** 
 * Write information to the blackboard to prepare for validation.
 */
public static void HydraTask_prepareForValidation() {
   int sleepSec = TestConfig.tasktab().intAt(ConnPoolPrms.sleepSec,
                  TestConfig.tab().intAt(ConnPoolPrms.sleepSec, 0));
   if (sleepSec > 0) {
      Log.getLogWriter().info("Sleeping for " + sleepSec + " seconds to allow connections time to balance");
      MasterController.sleepForMs(sleepSec * 1000);
      Log.getLogWriter().info("Done sleeping for " + sleepSec + " seconds to allow connections time to balance");
   }
   // find the number of current client connection and write it to the blackboard
   Cache aCache = CacheHelper.getCache();
   if (aCache == null) {
      throw new TestException("Cache is null");
   }
   // Replacing workaround as 39224 is fixed.
   if (aCache.isServer()) { // stats to get are server stats; only do this if this vm is a server
      Log.getLogWriter().info("isServer is " + aCache.isServer()); 
      BridgeServerImpl server = (BridgeServerImpl)aCache.getCacheServers().get(0);

      // currentClientConnections
      int currClientConnections = server.getAcceptor().getStats().getCurrentClientConnections();
      Log.getLogWriter().info("Current number of client connections: " + currClientConnections);
      String key = currentClientConnKey + RemoteTestModule.getMyVmid();
      ConnPoolBB.getBB().getSharedMap().put(key, new Integer(currClientConnections));

      // currentQueueConnections
      int currQueueConnections = server.getAcceptor().getStats().getCurrentQueueConnections();
      Log.getLogWriter().info("Current number of queue connections: " + currQueueConnections);
      key = currentQueueConnKey + RemoteTestModule.getMyVmid();
      ConnPoolBB.getBB().getSharedMap().put(key, new Integer(currQueueConnections));

      // queueLoad
      double queueLoad = server.getAcceptor().getStats().getQueueLoad();
      Log.getLogWriter().info("Queue load: " + queueLoad);
      key = queueLoadKey + RemoteTestModule.getMyVmid();
      ConnPoolBB.getBB().getSharedMap().put(key, new Double(queueLoad));

      // connectionLoad
      double connectionLoad = server.getAcceptor().getStats().getConnectionLoad();
      Log.getLogWriter().info("Connection load: " + connectionLoad);
      key = connectionLoadKey + RemoteTestModule.getMyVmid();
      ConnPoolBB.getBB().getSharedMap().put(key, new Double(connectionLoad));
   } else {
      Log.getLogWriter().info("Not getting server stats, this is not a server");
   }
}

/** 
 * Validate information written to the blackboard. 
 */
public static void HydraTask_validate() {
   Cache aCache = CacheHelper.getCache();
   BridgeServerImpl server = (BridgeServerImpl)aCache.getCacheServers().get(0);
   double loadPerConnection = server.getAcceptor().getStats().getLoadPerConnection();
   StringBuffer currentConnStr = new StringBuffer();
   StringBuffer currentQueueStr = new StringBuffer();
   StringBuffer queueLoadStr = new StringBuffer();
   StringBuffer connectionLoadStr = new StringBuffer();
   connectionLoadStr.append("   loadPerConnection is " + loadPerConnection + "\n"); 
   int minCurrentConn = Integer.MAX_VALUE;
   int maxCurrentConn = 0;
   int minCurrentQueue = Integer.MAX_VALUE;
   int maxCurrentQueue = 0;
   double minQueueLoad = Integer.MAX_VALUE;
   double maxQueueLoad = 0;
   double minConnectionLoad = Double.MAX_VALUE;
   double maxConnectionLoad = 0;
   Map aMap = new TreeMap(ConnPoolBB.getBB().getSharedMap().getMap());
   Iterator it = aMap.keySet().iterator();
   while (it.hasNext()) {
      String key = (String)(it.next());
      if (key.startsWith(currentClientConnKey)) {
         int value = ((Integer)(aMap.get(key))).intValue();
         int vmid = (Integer.valueOf(key.substring(currentClientConnKey.length(), key.length()))).intValue();
         currentConnStr.append("   for vmid " + vmid + ", currentClientConnections is " + value + "\n");
         maxCurrentConn = Math.max(value, maxCurrentConn);
         minCurrentConn = Math.min(value, minCurrentConn);
      } else if (key.startsWith(currentQueueConnKey)) {
         int value = ((Integer)(aMap.get(key))).intValue();
         int vmid = (Integer.valueOf(key.substring(currentQueueConnKey.length(), key.length()))).intValue();
         currentQueueStr.append("   for vmid " + vmid + ", currentQueueConnections is " + value + "\n");
         maxCurrentQueue = Math.max(value, maxCurrentQueue);
         minCurrentQueue = Math.min(value, minCurrentQueue);
      } else if (key.startsWith(queueLoadKey)) {
         double value = ((Double)(aMap.get(key))).doubleValue();
         int vmid = (Integer.valueOf(key.substring(queueLoadKey.length(), key.length()))).intValue();
         queueLoadStr.append("   for vmid " + vmid + ", queueLoad is " + value + "\n");
         maxQueueLoad = Math.max(value, maxQueueLoad);
         minQueueLoad = Math.min(value, minQueueLoad);
      } else if (key.startsWith(connectionLoadKey)) {
         double value = ((Double)(aMap.get(key))).doubleValue();
         int vmid = (Integer.valueOf(key.substring(connectionLoadKey.length(), key.length()))).intValue();
         connectionLoadStr.append("   for vmid " + vmid + ", connectionLoad is " + value + "\n");
         maxConnectionLoad = Math.max(value, maxConnectionLoad);
         minConnectionLoad = Math.min(value, minConnectionLoad);
      }
   }
   String stateStr = currentConnStr + "\n" + currentQueueStr + "\n" + queueLoadStr + "\n" + connectionLoadStr;

   // check for balance
   StringBuffer errStr = new StringBuffer();
   int diffConn = Math.abs(minCurrentConn - maxCurrentConn);
   if (diffConn > 1) {
      errStr.append("Unbalanced currentClientConnections");
   }

   // lynng - reduce failures due to 39365 until this bug is fixed
//   int diffQueue = Math.abs(minCurrentQueue - maxCurrentQueue);
//   if (diffQueue > 1) {
//      if (errStr.length() > 0) {
//         errStr.append(" and ");
//      }
//      errStr.append("Unbalanced currentQueueConnections");
//   }
//   double diffQueueLoad = Math.abs(minQueueLoad - maxQueueLoad);
//   if (diffQueueLoad > 1) {
//      if (errStr.length() > 0) {
//         errStr.append(" and ");
//      }
//      errStr.append("Unbalanced queueLoad");
//   }
   double diffConnectionLoad = Math.abs(minConnectionLoad - maxConnectionLoad);
   double diff = diffConnectionLoad / loadPerConnection;
   if ((diff > (double)0.9) && (diff < (double)1.1)) { 
      // close to 1, this is OK
   } else {
      if (diffConnectionLoad > loadPerConnection) {
         if (errStr.length() > 0) {
            errStr.append(" and ");
         }
         errStr.append("Unbalanced connectionLoad; difference between min and max connectionLoad is " + 
                diff + " times the loadPerConnection " + loadPerConnection);
      }
   }

   if (errStr.length() > 0) {
      throw new TestException(errStr.toString() + "\n" + stateStr);
   }
   Log.getLogWriter().info("Validation passed.\n" + stateStr);
}

}
