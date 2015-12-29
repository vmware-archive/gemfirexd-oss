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
package hct.ha;

import java.util.HashMap;
import java.util.Iterator;

import hydra.DistributedSystemHelper;
import hydra.GemFireDescription;
import hydra.Log;
import hydra.TestConfig;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ha.HAHelper;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.HaHelper;

/**
 * It is listener class which collects the statistics after the last_key is put
 * at the server side.
 *
 * @author Girish Thombare
 *
 */
public class HAServerEventListener extends CacheListenerAdapter
{
  public void afterCreate(EntryEvent event)
  {
    if (event.getKey().equals(Feeder.LAST_KEY)) {
      Cache cache = event.getRegion().getCache();
      Iterator itr = cache.getCacheServers().iterator();
      BridgeServerImpl server = (BridgeServerImpl)itr.next();
      Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
          .getClientProxies().iterator();
      while (iter_prox.hasNext()) {
        CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
        if (HaHelper.checkPrimary(proxy)) {
          ClientProxyMembershipID proxyID = proxy.getProxyID();
          Log.getLogWriter().info("Proxy id : " + proxyID.toString());
          HARegion region = (HARegion) proxy.getHARegion();
          HARegionQueue haRegionQueue = HAHelper.getRegionQueue(region);
          HashMap statMap = new HashMap();
          statMap.put("eventsPut", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsEnqued()));
          statMap.put("eventsConflated", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsConflated()));
          statMap.put("eventsRemoved", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsRemoved()));
          statMap.put("eventsExpired", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsExpired()));
          statMap.put("eventsRemovedByQRM", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsRemovedByQrm()));
          statMap.put("NumSequenceViolated", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getNumSequenceViolated()));
          statMap.put("eventsTaken", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsTaken()));
          // Log.getLogWriter().info("statts remove : " +
          // HAHelper.getRegionQueueStats(
          // haRegionQueue).getNumVoidRemovals());
          statMap.put("numVoidRemovals", new Long(HAHelper.getRegionQueueStats(
              haRegionQueue).getNumVoidRemovals()));
          Log.getLogWriter().info("StatMap  : " + statMap.toString());
          String proxyIdStr = proxyID.toString();
          // Use only process id part of proxyIdstr when Network Partition
          // Detection on server is enabled.
          // as hostname part going to return differnt values on server and
          // client.
          if (DistributedSystemHelper.getGemFireDescription()
              .getEnableNetworkPartitionDetection()) {
            proxyIdStr = proxyIdStr.substring(proxyIdStr.lastIndexOf("(") + 1,
                proxyIdStr.indexOf(":"));
          }
          HAClientQueueBB.getBB().getSharedMap().put(proxyIdStr, statMap);
          
          HAClientQueueBB.getBB().getSharedCounters().add(
              HAClientQueueBB.NUM_GLOBAL_CONFLATE, (HAHelper.getRegionQueueStats(
              haRegionQueue).getEventsConflated()));
        }
      }
    }
  }

  public static void putHAStatsInBlackboard()
  {
    //if (event.getKey().equals("last_key")) {
    //Validator.pauseBeforeValidation();

    try {
      Thread.sleep(240000);
    }
    catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Cache cache = GemFireCacheImpl.getInstance();
    Iterator itr = cache.getCacheServers().iterator();
    BridgeServerImpl server = (BridgeServerImpl)itr.next();
    Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
        .getClientProxies().iterator();
    while (iter_prox.hasNext()) {
      CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
      if (HaHelper.checkPrimary(proxy)) {
        ClientProxyMembershipID proxyID = proxy.getProxyID();
        Log.getLogWriter().info("Proxy id : " + proxyID.toString());
        HARegion region = (HARegion) proxy.getHARegion();
        HARegionQueue haRegionQueue = HAHelper.getRegionQueue(region);
        HashMap statMap = new HashMap();
        statMap.put("eventsPut", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getEventsEnqued()-1));     // subtract durableClient MarkerMessage
        statMap.put("eventsConflated", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getEventsConflated()));
        statMap.put("eventsRemoved", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getEventsRemoved()-1));    // subtract durableClient Marker Message
        statMap.put("eventsExpired", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getEventsExpired()));
        statMap.put("eventsRemovedByQRM", new Long(HAHelper
            .getRegionQueueStats(haRegionQueue).getEventsRemovedByQrm()));
        statMap.put("NumSequenceViolated", new Long(HAHelper
            .getRegionQueueStats(haRegionQueue).getNumSequenceViolated()));
        statMap.put("eventsTaken", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getEventsTaken()));
        // Log.getLogWriter().info("statts remove : " +
        // HAHelper.getRegionQueueStats(
        // haRegionQueue).getNumVoidRemovals());
        statMap.put("numVoidRemovals", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getNumVoidRemovals()));
        Log.getLogWriter().info("new StatssMap  : " + statMap.toString());
        HAClientQueueBB.getBB().getSharedMap().put(proxyID.toString(), statMap);
        HAClientQueueBB.getBB().getSharedCounters().add(
            HAClientQueueBB.NUM_GLOBAL_CONFLATE,
            (HAHelper.getRegionQueueStats(haRegionQueue).getEventsConflated()));
      }
    }
    //}
  }

}
