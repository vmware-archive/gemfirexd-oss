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
package roles;

import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.internal.cache.*;

/**
 * Hydra tasks and support methods for testing Roles and Reliability.
 * 
 * @see hydra.GemFirePrms.roles
 * @see util.ReliabilityPrms
 * @see roles.RolesBB (reliableQueuedOps, actualQueuedEvents)
 *
 * @author lhughes
 * @since 5.0
 */
public class RolesTest {

  /**
   * Hydra ENDTASK to compare the total cachePerfStats.reliableQueuedOps
   * to the number of events (isQueued()==true) received.
   *
   * @see RolesBB.expectedQueuedEvents
   * @see RolesBB.actualQueuedEvents
   *
   */
  public static void EndTask_validateReliableQueuedOps() {

    long expected = RolesBB.getBB().getSharedCounters().read(RolesBB.expectedQueuedEvents);
    long actual = RolesBB.getBB().getSharedCounters().read(RolesBB.actualQueuedEvents);

    if (actual < expected) {
      throw new TestException("Expected to receive at least " + expected + " queued Operations, but received " + actual);
    }
    Log.getLogWriter().info("reliableOps delivery validated: expected at least " + expected + " events, received " + actual);
  }

  /**
   * Hydra ENDTASK to compare the expected RoleLoss & RoleGain events
   * to the actual number of Role Loss/Gain events received
   *
   * @see RolesBB.expectedRoleLossEvents
   * @see RolesBB.actualRoleLossEvents
   * @see RolesBB.expectedRoleGainEvents
   * @see RolesBB.actualRoleGainEvents
   *
   */
  public static void EndTask_validateRoleChangeEvents() {
    SharedCounters sc = RolesBB.getBB().getSharedCounters();
    // RoleLoss Events (plus one for the task client vm shutdown
    long expected = sc.read(RolesBB.expectedRoleLossEvents);
    expected = expected * (TestHelper.getNumVMs()-1);

    long actual = sc.read(RolesBB.actualRoleLossEvents);

    if (actual < expected) {
       throw new TestException("Expected to receive at least " + expected + " RoleLoss events, but instead received " + actual);
    }

    Log.getLogWriter().info("RoleLossEvents delivery validated: expected at least " + expected + " events, received " + actual);

    // RoleGain Events
    expected = sc.read(RolesBB.expectedRoleGainEvents);
    expected = expected * (TestHelper.getNumVMs()-1);

    actual = sc.read(RolesBB.actualRoleGainEvents);

    if (actual < expected) {
       throw new TestException("Expected to receive " + expected + " RoleGain events, but instead received " + actual);
    }

    Log.getLogWriter().info("RoleGainEvents delivery validated: expected at least " + expected + " events, received " + actual);
  }

  /**
   * Hydra ENDTASK to validate that we processed some number of 
   * RegionAccessExceptions and RegionDistributionExceptions (when expected)
   */
  public static void EndTask_validateRegionRoleExceptionsProcessed() {
    SharedCounters sc = RolesBB.getBB().getSharedCounters();
    long regionAccessExceptions = sc.read(RolesBB.regionAccessExceptions);
    long regionDistributionExceptions = sc.read(RolesBB.regionDistributionExceptions);

    StringBuffer aStr = new StringBuffer();
    if (regionAccessExceptions == 0) {
      aStr.append("Test may not have caused any RegionAccessExceptions: regionAccessExceptions = " + regionAccessExceptions + "\n");
    }

    if (regionDistributionExceptions == 0) {
      aStr.append("Test may not have caused any RegionDistributionExceptions: regionDistributionExceptions = " + regionDistributionExceptions + "\n");
    }

    if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
    }

    Log.getLogWriter().info("Test processed " + regionAccessExceptions + " RegionAccessExceptions and " + regionDistributionExceptions + " RegionDistributionExceptions");
  }

  /**
   * Hydra TASK to stop a randomly selected client, then dynamically start
   * any available clients (those with startMode = on_demand).  
   */
  public static void stopAndStartClient() throws ClientVmNotFoundException {
     // stop and start are synchronous here, so we know things are either
     // up or down.  Sleep in between to allow detection of requiredRoles changes
     stopClient();
     MasterController.sleepForMs(10000);

     long counter = RolesBB.getBB().getSharedCounters().add(RolesBB.expectedQueuedEvents, (long)getNumQueuedOps());
     Log.getLogWriter().info("After incrementing counter, expectedQueuedEvents = " + counter);
     startClient();
     MasterController.sleepForMs(10000);
  }
    
  /**
   * Asynchronously stops a random client using stopMode and startMode from the
   * test prms.  client to stop is determined via RolesPrms.clientsToStop.
   *
   * @see RolesPrms.clientsToStop
   * @see RolesPrms.stopMode
   * @see RoelsPrms.startMode
   */
  public static void stopClientAsync() throws ClientVmNotFoundException {
    String stopMode = TestConfig.tab().stringAt( RolesPrms.stopMode, "MEAN_KILL" );
    String startMode = TestConfig.tab().stringAt( RolesPrms.startMode, "ON_DEMAND" );
    Long key = RolesPrms.clientsToStop;
    String clientToStop = TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key));
    ClientVmInfo info = ClientVmMgr.stopAsync(
                             "stop client", 
                             ClientVmMgr.toStopMode( stopMode ),
                             ClientVmMgr.toStartMode( startMode ),
                             new ClientVmInfo(null, clientToStop, null)
                        );
    Log.getLogWriter().info("stopped client " + info);
    long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.expectedRoleLossEvents);
    Log.getLogWriter().info("after incrementing counter, RolesBB.expectedRoleLossEvents = " + count);
  }
                                                                                   
  /**
   *  Start any available client (one stopped with startMode = on_demand)
   */
  public static void startClientAsync() throws ClientVmNotFoundException {
    try {
      ClientVmInfo info = ClientVmMgr.startAsync("start client");
    } catch( ClientVmNotFoundException e ) {
      Log.getLogWriter().info("Could not start consumer, perhaps no vms waiting to be started, continuing");
    }
    long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.expectedRoleGainEvents);
    Log.getLogWriter().info("after incrementing counter, RolesBB.expectedRoleGainEvents = " + count);
  }

  /**
   * stop a random clients (synchrnously).  Control does not return to the caller
   * until the client has completed stopped.  stopMode and startMode are determined
   * by RolesPrms.
   *
   * @see RolesPrms.clientsToStop
   * @see RolesPrms.stopMode
   * @see RoelsPrms.startMode
   */
  public static void stopClient() throws ClientVmNotFoundException {
    String stopMode = TestConfig.tab().stringAt(RolesPrms.stopMode, "MEAN_KILL" );
    String startMode = TestConfig.tab().stringAt(RolesPrms.startMode, "ON_DEMAND" );
    Long key = RolesPrms.clientsToStop;
    String clientToStop = TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key));
    ClientVmInfo info = ClientVmMgr.stop(
                             "stop client", 
                             ClientVmMgr.toStopMode( stopMode ),
                             ClientVmMgr.toStartMode( startMode),
                             new ClientVmInfo(null, clientToStop, null)
                        );
    Log.getLogWriter().info("stopped client" + info);
    long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.expectedRoleLossEvents);
    Log.getLogWriter().info("after incrementing counter, RolesBB.expectedRoleLossEvents = " + count);
  }
                                                                                   
  /**
   * Starts any available VM (synchronously).  Control does not return to the caller
   * until the client has completed started up.
   */
  public static void startClient() throws ClientVmNotFoundException {
    try {
      ClientVmInfo info = ClientVmMgr.start("start client");
    } catch( ClientVmNotFoundException e ) {
      Log.getLogWriter().info("Could not start client!");
    }
    long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.expectedRoleGainEvents);
    Log.getLogWriter().info("after incrementing counter, RolesBB.expectedRoleGainEvents = " + count);
  }

  /** Return the cachePerfStat.reliableQueuedOps
   *
   *  @return The number of reliableQueuedOps, obtained from cacheperfstats
   *
   */
  public static double getNumQueuedOps() {
     GemFireCacheImpl myCache = (GemFireCacheImpl)CacheUtil.getCache();
     CachePerfStats stats = myCache.getCachePerfStats();
     int reliableQueuedOps = stats.getReliableQueuedOps();
     Log.getLogWriter().info("GemFireCache.getCachePerfStats().getReliableQueuedOps() returns " + reliableQueuedOps);
     return reliableQueuedOps;
  }
}
