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

import hct.BBoard;
import hct.HctPrms;
import hydra.*;
import hydra.blackboard.SharedCounters;

import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import query.QueryPrms;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

public class HACache
{

  static ConfigHashtable conftab = TestConfig.tab();

  static LogWriter logger = Log.getLogWriter();

  static BBoard bb = BBoard.getInstance();

  static String regionName = conftab.stringAt(HctPrms.regionName);

  static GsRandom rand = new GsRandom();

  static int numClients = -1; // lazily initialized

  static long killInterval = conftab.longAt(HctPrms.killInterval);

  /**
   * A Hydra TASK that kills a random stable server.  Uses a binary semaphore
   * initialized by {@link EmptyQueueListener#setStableStateAchievedAndSleep}
   * to serialize killing.
   *
   * @see #killServer
   */
  public static synchronized void killStableServer()
  throws ClientVmNotFoundException {
    SharedCounters counters = HAClientQueueBB.getBB().getSharedCounters();
    Log.getLogWriter().info("Waiting for stable signal...");
    while (true) { // poll, but not too busily
      MasterController.sleepForMs(3000);
      if (counters.decrementAndRead(HAClientQueueBB.stableSignal) < 0) {
        // it was zero when read, so put it back and keep waiting, making it
        // possible in theory, but unlikely in practice, to encounter livelock
        counters.increment(HAClientQueueBB.stableSignal);
      } else {
        Log.getLogWriter().info("Got stable signal...");
        killServer(); // kill a random server
        Log.getLogWriter().info("Setting stable signal...");
        counters.increment(HAClientQueueBB.stableSignal); // notify others
        return;
      }
    }
  }

  /**
   * A Hydra TASK that kills a random server
   */
  public synchronized static void killServer() throws ClientVmNotFoundException
  {

    Set dead;
    Set active;
    Region aRegion = null; 
    if(TestConfig.tab().booleanAt(QueryPrms.regionForRemoteOQL,false))
    {
      aRegion = RegionHelper.getRegion(regionName);
    } else 
    {
      int numRegion = TestConfig.tab().intAt(HAClientQueuePrms.regionRange,1)-1;
      aRegion = RegionHelper.getRegion(regionName+numRegion);
    }
    
    Assert.assertNotNull(aRegion);

    active = ClientHelper.getActiveServers(aRegion);

    // keep at least 2 servers alive
    // TODO : get this from conf file
    int minServersRequiredAlive = TestConfig.tab().intAt(
        HAClientQueuePrms.minServersRequiredAlive, 3);
    if (active.size() < minServersRequiredAlive) {
      logger.info("No kill executed , a minimum of " + minServersRequiredAlive
          + " servers have to be kept alive");
      return;
    }

    long now = System.currentTimeMillis();
    Long lastKill = (Long)bb.getSharedMap().get("lastKillTime");
    long diff = now - lastKill.longValue();
    
    if (diff < killInterval) {
      logger.info("No kill executed");
      return;
    }
    else {
      bb.getSharedMap().put("lastKillTime", new Long(now));
    }

    // Kill a random server
    List endpoints = BridgeHelper.getEndpoints();
    int index = rand.nextInt(endpoints.size() - 1);
    BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints.get(index);
    ClientVmInfo target = new ClientVmInfo(endpoint);
    target = ClientVmMgr.stop("Killing random cache server",
             ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND, target);
    Log.getLogWriter().info("Server Killed : " + target);
    // Each client VM should receive a server memberCrashed event
    long count = BBoard.getInstance().getSharedCounters().add(
        BBoard.expectedServerCrashedEvents, getNumClients());
    Log.getLogWriter().info(
        "After incrementing, BBoard.expectedServerCrashedEvents = " + count);

    // sleep
    int sleepSec = TestConfig.tab().intAt(HctPrms.restartWaitSec);
    logger.info("Sleeping for " + sleepSec + " seconds");
    MasterController.sleepForMs(sleepSec * 1000);

    // do a get to trigger failover. first local destroy the entry to make
    // sure that the get misses teh cache at first

    // Iterator iterator = aRegion.keys().iterator();
    // if (iterator.hasNext()) {
    // Object key = iterator.next();
    // try {
    // aRegion.localDestroy(key);
    // //aRegion.get(key);
    // }
    // catch (Exception e) {
    // logger
    // .info(
    // " Exception occured while trying to get to initiate failover due to ",
    // e);
    // }
    // }

    active = ClientHelper.getActiveServers(aRegion);

    ServerLocation server = new ServerLocation(endpoint.getHost(), endpoint.getPort());
    if (active.contains(server)) {
      logger.info("ERROR: Killed server " + server
                  + " found in Active Server List: " + active);
    }

    ClientVmMgr.start("Restarting the cache server", target);
    // each client VM should receive a server memberJoined event
    count = BBoard.getInstance().getSharedCounters().add(
        BBoard.expectedServerJoinedEvents, getNumClients());
    Log.getLogWriter().info(
        "After incrementing, BBoard.expectedServerJoinedEvents = " + count);
    // clients join (get a new socket) when the servers come back up, so
    // we need to expect clientJoined events as well
    count = BBoard.getInstance().getSharedCounters().add(
        BBoard.expectedClientJoinedEvents, endpoints.size());
    Log.getLogWriter().info(
        "After incrementing, BBoard.expectedClientJoinedEvents = " + count);

    int sleepMs = ClientHelper.getRetryInterval(aRegion) + 1000;
    logger.info("Sleeping for " + sleepMs + " ms");
    MasterController.sleepForMs(sleepMs);

    // iterator = aRegion.keys().iterator();
    // if (iterator.hasNext()) {
    // Object key = iterator.next();
    // try {
    // aRegion.localDestroy(key);
    // //aRegion.get(key);
    // }
    // catch (Exception e) {
    // logger
    // .info(
    // " Exception occured while trying to get to initiate failover due to ",
    // e);
    // }
    // }

    active = ClientHelper.getActiveServers(aRegion);
    if (!active.contains(server)) {
      // throw new HydraRuntimeException("ERROR: Restarted server "
      logger.info("ERROR: Restarted server " + server
                  + " not in Active Server List: " + active);
    }

    return;
  }

  /**
   * A Hydra TASK that kills a client
   */
  public synchronized static void killClient()
  {
    if (HAClientQueueBB.getBB().getSharedCounters().incrementAndRead(
        HAClientQueueBB.NUM_CLIENTS_KILL) <= TestConfig.tab().intAt(
        HAClientQueuePrms.maxClientsCanKill, 1)) {
      if (DistributedSystemHelper.getDistributedSystem() != null) {
        // close the cache and disconnect from the distributed system
        CacheHelper.closeCache();
        //DistributedSystemHelper.getDistributedSystem().disconnect();
      }

      MasterController.sleepForMs(5000);
      HAClientQueue.initCacheClient();
      HAClientQueueBB.getBB().getSharedCounters().decrement(
          HAClientQueueBB.NUM_CLIENTS_KILL);
      logger.info("Restarted Client. No of clients down are "
          + HAClientQueueBB.getBB().getSharedCounters().read(
              HAClientQueueBB.NUM_CLIENTS_KILL));
    }
    else
    {      
      HAClientQueueBB.getBB().getSharedCounters().decrement(
          HAClientQueueBB.NUM_CLIENTS_KILL);
      
      logger.info("No of clients down are "
          + HAClientQueueBB.getBB().getSharedCounters().read(
              HAClientQueueBB.NUM_CLIENTS_KILL));
    }
      
  }

  public static synchronized int getNumClients() {
    if (numClients == -1) {
      int totalVMs = TestConfig.getInstance().getTotalVMs();
      int bridgeVMs = BridgeHelper.getEndpoints().size();
      numClients = totalVMs - bridgeVMs;

      Log.getLogWriter().info("numBridgeServers = " + bridgeVMs);
      Log.getLogWriter().info("numEdgeClients = " + numClients);
    }
    return numClients;
  }
}
