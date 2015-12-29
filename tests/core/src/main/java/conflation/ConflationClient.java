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

package conflation;

import cacheperf.*;
import com.gemstone.gemfire.cache.*;
import distcache.gemfire.*;
import hydra.*;

import com.gemstone.gemfire.internal.InternalInstantiator;
import java.util.*;
import objects.*;
import perffmwk.*;
import util.*;

/**
 * Client for conflation testing.
 */
public class ConflationClient extends CachePerfClient {

  //----------------------------------------------------------------------------
  //  Gateway Hub Tasks
  //----------------------------------------------------------------------------

  /**
   * Creates a gateway hub as required by {@link #startGatewayHubTask}.
   */
  public static void createGatewayHubTask() {
    GatewayHubHelper.createGatewayHub("hub");
  }

  /**
   * Creates gateways and starts the hub.  Assumes {@link #createGatewayHubTask}
   * has already executed.
   */
  public static void startGatewayHubTask() {
    GatewayHubHelper.addGateways("gateway");
    GatewayHubHelper.startGatewayHub();
  }

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * TASK to register interest.  Registers a total of {@link
   * ConflationPrms#interestTotalSize} keys, {@link
   * ConflationPrms#interestBatchSize} at a time, using the key allocation
   * set by {@link cacheperf.CacheClientPrms#keyAllocationType}.
   */
  public static void registerInterestTask() {
    ConflationClient c = new ConflationClient();
    c.initialize();
    c.registerInterest();
  }
  private void registerInterest() {
    int interestTotalSize = ConflationPrms.getInterestTotalSize();
    int interestBatchSize = ConflationPrms.getInterestBatchSize();
    Region region = ((GemFireCacheTestImpl)this.cache).getRegion();
    List interestList = new ArrayList();
    try {
      for (int i = 0; i <= interestTotalSize; i++) {
        int name = getNextKey();
        Object key = ObjectHelper.createName(name);
        interestList.add(key);
        if (interestList.size() % interestBatchSize == 0) {
          // register the batch
          region.registerInterest(interestList);
          Log.getLogWriter().info("Registering " + interestList);
          interestList = new ArrayList();
        }
        ++this.count;
        ++this.keyCount;
      }
      // register the remainder, if any
      if (interestList.size() != 0) {
        region.registerInterest(interestList);
        Log.getLogWriter().info("Registering " + interestList);
      }
    } catch (CacheWriterException e) {
      String s = "Failed to register interest in " + interestList;
      throw new HydraRuntimeException(s, e);
    } 
  }

  /**
   * TASK to feed objects for {@link ConflationPrms#taskGranularityMs}.  If
   * {@link ConflationPrms.feedMonotonically} is true, the object values are
   * increased monotonically by this feed thread.  If all feeds use an "own" key
   * allocation type, the values are guaranteed to increase monotonically.  If
   * feeds use a "same" key allocation type, it is possible that concurrent
   * updates to the same key will allow values to be non-monotonic at a client,
   * even with full mirroring, unless global scope is used.  Listener validation
   * can be turned on using {@link ConflationPrms#validateMonotonic}.
   *
   * The task can also be configured to do intermittent destroys via {@link
   * ConflationPrms#destroyPercentage}.
   */
  public static void feedDataTask() {
    ConflationClient c = new ConflationClient();
    c.initialize(PUTS);
    c.feedData();
  }
  private void feedData() {
    String objectType = CachePerfPrms.getObjectType();
    boolean feedMonotonically = ConflationPrms.feedMonotonically();
    int destroyPercentage = ConflationPrms.getDestroyPercentage();
    long endTime = System.currentTimeMillis()
                 + ConflationPrms.getTaskGranularityMs();
    int puts = 0;
    do {
      int name = getNextKey();
      Object key = ObjectHelper.createName(name);
      int n = this.rng.nextInt(1, 100);
      if (n < destroyPercentage && this.cache.get(key) != null) {
        destroy(key);
        if (feedMonotonically) {
          name = 0; // reset the index
        }
        Object val = ObjectHelper.createObject(objectType, name);
        create(key, val);
      } else {
        if (feedMonotonically) {
          Object oldVal = this.cache.get(key);
          if (oldVal == null) {
            name = 0; // set the starting index
          } else { // bump the index by 1 from its last value
            name = ObjectHelper.getIndex(oldVal) + 1;
          }
        }
        Object val = ObjectHelper.createObject(objectType, name);
        put(key, val);
        ++puts;
      }
      if (feedMonotonically) {
        TestHelper.checkForEventError(ConflationBlackboard.getInstance());
      }
    } while (System.currentTimeMillis() < endTime);
    ConflationBlackboard.getInstance().getSharedCounters()
                        .add(ConflationBlackboard.Puts, puts);
  }
  private void put(Object key, Object val) {
    long start = this.statistics.startPut();
    this.cache.put(key, val);
    this.statistics.endPut(start, this.isMainWorkload, this.histogram);
    ++this.count;
    ++this.keyCount;
  }
  private void create(Object key, Object val) {
    long start = this.statistics.startCreate();
    this.cache.create(key, val);
    this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
    ++this.count;
    ++this.keyCount;
  }
  private void destroy(Object key) {
    long start = this.statistics.startDestroy();
    this.cache.destroy(key);
    this.statistics.endDestroy(start, this.isMainWorkload, this.histogram);
    ++this.count;
    ++this.keyCount;
  }

  /**
   * TASK to wait until all create events have been received.
   */
  public static void waitForCreateEventsTask() {
    ConflationClient c = new ConflationClient();
    c.waitForCreateEvents();
  }
  private void waitForCreateEvents() {
    int numVMs = numVMsInThreadGroup();
    long lastReceived = -1;
    String target, status;
    int maxEventWaitMs = ConflationPrms.getMaxEventWaitSec() *1000;
    long endTime = System.currentTimeMillis() + maxEventWaitMs;
    while (true) {
      long creates        = getStatTotalCount("cacheperf.CachePerfStats",
                                              "creates");
      long createEvents   = getStatTotalCount("cacheperf.CachePerfStats",
                                              "createEvents");
      long expected       = creates * numVMs;
      long received       = createEvents;

      target = ": expected " + expected + " (as " + creates + " creates * " + numVMs + " vms)";
      status = ", received " + received + " (as " + createEvents + " create events)";

      if (received == expected) {
        log().info("Did" + target + status);
        return;
      } else if (received != lastReceived) {
        log().info("Waiting for" + target + status);
        lastReceived = received;
        MasterController.sleepForMs(5000);
        endTime = System.currentTimeMillis() + maxEventWaitMs;
      } else if (System.currentTimeMillis() > endTime) {
        throw new HydraRuntimeException("Timed out" + target + status);
      }
    }
  }

  /**
   * TASK to wait until all messages have been delivered.
   */
  public static void waitForConflatedUpdateEventsTask() {
    ConflationClient c = new ConflationClient();
    c.waitForConflatedUpdateEvents();
  }
  private void waitForConflatedUpdateEvents()
  {
    int numVMs = numVMsInThreadGroup();
    long lastReceived = -1;
    String target, status;
    long puts = ConflationBlackboard.getInstance().getSharedCounters().read(
        ConflationBlackboard.Puts);
    String client = RemoteTestModule.getMyClientName();
    int clientVmsPerSite = TestConfig.getInstance()
        .getClientDescription(client).getVmQuantity();
    int maxEventWaitMs = ConflationPrms.getMaxEventWaitSec() *1000;
    long endTime = System.currentTimeMillis() + maxEventWaitMs;
    while (true) {
      long eventsConflated = 0 ; 
      long eventsRemoved = 0 ; 
      long eventsTaken = 0 ;
      long eventsExpired = 0 ;
      long eventsRemovedByQrm = 0 ;
      long wanconflations = 0;
      long updateEvents = 0 ;
      long creates = 0 ;
      long destroys = 0;
      long expectedMarkerMessages = 0;   // durableClient MarkerMessages 
      
      if (TestConfig.tab().booleanAt(hydra.RegionPrms.enableGateway, false) 
          || TestConfig.tab().stringAt(hydra.RegionPrms.gatewaySenderNames, null) != null){
        updateEvents   = getStatTotalCount("cacheperf.CachePerfStats",
                                              "updateEvents");        
        if (TestConfig.tab().booleanAt(hydra.GatewayPrms.batchConflation, false)){
          wanconflations = getStatTotalCount("GatewayStatistics",  "eventsNotQueuedConflated");          
        }else if (TestConfig.tab().booleanAt(hydra.GatewaySenderPrms.batchConflationEnabled, false)){
          wanconflations = getStatTotalCount("GatewaySenderStatistics",  "eventsNotQueuedConflated");
        }
      } else {
        creates = getStatTotalCount("cacheperf.CachePerfStats", "creates");
        destroys = getStatTotalCount("cacheperf.CachePerfStats", "destroys");
        eventsConflated = getStatTotalCount("ClientSubscriptionStats", "eventsConflated");
        eventsRemoved = getStatTotalCount("ClientSubscriptionStats", "eventsRemoved");
        eventsTaken = getStatTotalCount("ClientSubscriptionStats", "eventsTaken");
        eventsExpired = getStatTotalCount("ClientSubscriptionStats", "eventsExpired");
        eventsRemovedByQrm = getStatTotalCount("ClientSubscriptionStats", "eventsRemovedByQrm");
        expectedMarkerMessages = clientVmsPerSite;
      }
      long instantiatorsCnt = InternalInstantiator.getInstantiators().length ;
      long expected  = (creates + puts + destroys ) * numVMs + 
                        instantiatorsCnt * clientVmsPerSite + 
                        expectedMarkerMessages;
      long received = updateEvents + eventsConflated + eventsRemoved + eventsTaken
          + eventsExpired + eventsRemovedByQrm + wanconflations * clientVmsPerSite;

      target = ": expected " + expected + " as (" + creates + " creates plus "
          + puts + " puts plus " + destroys + " destroys )" + " * " + numVMs
          + " vms";
      status = ", received " + received + " (as " + updateEvents
          + " updateEvents plus " + eventsConflated
          + " eventsConflated plus " + eventsRemoved + " eventsRemoved plus "
          + eventsTaken + " eventsTaken plus " + eventsExpired
          + " eventsExpired plus " + eventsRemovedByQrm
          + " eventsRemovedByQrm plus " + wanconflations
          + " total wan conflations * " + clientVmsPerSite + " clientVmsPerSite plus "
          + expectedMarkerMessages + " durableClient Marker messages)";

      if (received == expected) {
        log().info("Did" + target + status);
        return;
      }
      else if (received != lastReceived) {
        log().info("Waiting for" + target + status);
        lastReceived = received;
        MasterController.sleepForMs(5000);
        endTime = System.currentTimeMillis() + maxEventWaitMs;
      }
      else if (System.currentTimeMillis() > endTime) {
        throw new HydraRuntimeException("Timed out" + target + status);
      }
    }
  }

  private int numVMsInThreadGroup() {
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    HydraThreadGroup tg = TestConfig.getInstance().getThreadGroup(tgname);
    Vector tsgs = tg.getSubgroups();
    int numvms = 0;
    for (Iterator i = tsgs.iterator(); i.hasNext();) {
      HydraThreadSubgroup tsg = (HydraThreadSubgroup)i.next();
      numvms += tsg.getTotalVMs();
    }
    return numvms;
  }

  /**
   * TASK to validate that messages have been conflated (or not) as expected.
   */
  public static void validateConflationStatsTask() {
    ConflationClient c = new ConflationClient();
    c.validateConflationStats();
  }
  private void validateConflationStats() {
    String type = ConflationPrms.getConflationStatType();
    String stat = ConflationPrms.getConflationStat();
    boolean expectConflation = ConflationPrms.expectNonZeroConflationStat();
    long conflations = getStatTotalCount(type, stat);
    String s = type + "." + stat + " = " + conflations;
    if (expectConflation) {
      if (conflations <= 0) {
        throw new HydraRuntimeException("No messages were conflated: " + s);
      } else {
        log().info(s);
      }
    } else {
      if (conflations > 0) {
        throw new HydraRuntimeException("Messages were conflated: " + s);
      } else {
        log().info(s);
      }
    }
  }

  /*
   * TASK to validate that no messages were dropped by bridge servers when
   * conflation is on.  This assumes that the number of distinct keys used
   * by the test is less than the queue size, otherwise messages could be
   * dropped in spite of conflation.
   */
  /*public static void validateMessagesFailedQueuedTask() {
    ConflationClient c = new ConflationClient();
    c.validateMessagesFailedQueued();
  }
  private void validateMessagesFailedQueued() {
    String type = "CacheClientProxyStatistics";
    String stat = "messagesFailedQueued";
    long failures = getStatTotalCount(type, stat);
    String s = type + "." + stat + " = " + failures;
    if (failures == 0 || !ConflationPrms.enableConflation()) {
      log().info(s);
    } else {
      throw new HydraRuntimeException("Messages were dropped: " + s);
    }
  }*/

  /**
   * Returns the total value of the stat, which it gets by combining the
   * unfiltered max value of the stat across all archives.
   */
  private long getStatTotalCount(String type, String stat) {
    String spec = "* " // search all archives
                + type + " "
                + "* " // match all instances
                + stat + " "
                + StatSpecTokens.FILTER_TYPE + "="
                        + StatSpecTokens.FILTER_NONE + " "
                + StatSpecTokens.COMBINE_TYPE + "="
                        + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List psvs = PerfStatMgr.getInstance().readStatistics(spec);
    if (psvs == null) {
      Log.getLogWriter().warning("No stats found for " + spec);
      return 0;
    } else {
      double total = 0;
      for (int i = 0; i < psvs.size(); i++) {
        PerfStatValue psv = (PerfStatValue)psvs.get(i);
        total += psv.getMax();
      }
      return (long)total;
    }
  }
}
