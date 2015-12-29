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
package delta;

import hct.BBoard;
import hct.HctPrms;
import hydra.BridgeHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigHashtable;
import hydra.GsRandom;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import query.QueryPrms;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * 
 * logic for crashing VM
 * 
 * @author aingle
 * @since 6.1
 */

public class DeltaCache {

  static ConfigHashtable conftab = TestConfig.tab();

  static LogWriter logger = Log.getLogWriter();

  static BBoard bb = BBoard.getInstance();

  static String regionName = conftab.stringAt(HctPrms.regionName);

  static long killInterval = conftab.longAt(HctPrms.killInterval);

  static GsRandom rand = new GsRandom();

  static int numClients = -1; // lazily initialized

  /**
   * A Hydra TASK that kills a random server
   */
  public synchronized static void killServer() throws ClientVmNotFoundException {
    Set active;
    Region aRegion = null;
    if (TestConfig.tab().booleanAt(QueryPrms.regionForRemoteOQL, false)) {
      aRegion = RegionHelper.getRegion(regionName);
    }
    else {
      int numRegion = TestConfig.tab().intAt(DeltaPropagationPrms.regionRange,
          1) - 1;
      aRegion = RegionHelper.getRegion(regionName + numRegion);
    }

    Assert.assertNotNull(aRegion);

    active = ClientHelper.getActiveServers(aRegion);

    // keep at least 2 servers alive
    // TODO : get this from conf file
    int minServersRequiredAlive = TestConfig.tab().intAt(
        DeltaPropagationPrms.minServersRequiredAlive, 3);
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
    BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints
        .get(index);
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

    active = ClientHelper.getActiveServers(aRegion);

    ServerLocation server = new ServerLocation(endpoint.getHost(), endpoint
        .getPort());
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
    active = ClientHelper.getActiveServers(aRegion);
    if (!active.contains(server)) {
      // throw new HydraRuntimeException("ERROR: Restarted server "
      logger.info("ERROR: Restarted server " + server
          + " not in Active Server List: " + active);
    }

    return;
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
  
 /**
  * Kill the clients
  * 
  */
 public static void killClient() {
   Log.getLogWriter().info(" VM Durable Client Id is " + DeltaPropagation.VmDurableId);
   // store that this client is once down
   DeltaPropagationBB.getBB().getSharedMap().put(DeltaPropagation.VmDurableId, "true");
    try {
      hydra.MasterController.sleepForMs(5000);
      synchronized (DeltaDurableClientValidationListener.latestValues) {
    	DeltaPropagationBB.getBB().getSharedMap().put(
            DeltaPropagation.VmDurableId + "keyValueMap",
            DeltaDurableClientValidationListener.durableKeyMap);
      }
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }
}
