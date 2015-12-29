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

import hct.HctPrms;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;

/**
 * A Feeder class for Delta testing. It creates a configurable ( passed
 * through bt/conf) number of threads for doing puts and each thread does a put
 * on fixed set of keys with range specified by configuration.
 * 
 * @author aingle
 * @since 6.1
 * 
 */
public class Feeder {
  /** Total number of perpetual put threads to be created */
  final static int TOTAL_PUT_THREADS = TestConfig.tab().intAt(
      DeltaPropagationPrms.numPutThreads);

  /** range of keys to be used by each thread */
  final static int PUT_KEY_RANGE = TestConfig.tab().intAt(
      DeltaPropagationPrms.numKeyRangePerThread);

  /** A static array containing the reference to the put threads */
  static Thread[] putThreads = new Thread[TOTAL_PUT_THREADS];

  /** A regionName to which index is attached to create multiple regions */
  final static String regionName = TestConfig.tab()
      .stringAt(HctPrms.regionName);

  /**
   * A list containing the counter for each thread. The counter value specifies
   * the number of times a put thread would operate on its range. The hydra task
   * thread will increment this value in each scheduling and the corresponding
   * put thread will decrement this value each time it operates on its keyset
   */
  static List threadList = new ArrayList(TOTAL_PUT_THREADS);

  /** A boolean to stop all the put threads. This will be toggled via CLOSETASK */
  static volatile boolean stop = false;

  /** A boolean to keep track if any exception occured in the put threads. */
  static volatile boolean exceptionOccured = false;
  
  /**
   * A stringbuffer to store messages if any exception that occured in the put
   * threads.
   */
  static StringBuffer exceptionMsg = new StringBuffer();

  /**
   * A Map which contains a thread-id as key and a map as value. This value map
   * contains latest values for the key-set of that particular thread.
   */
  protected static final Map latestValues = new HashMap();

  // test region name
  static protected final String REGION_NAME = TestConfig.tab().stringAt(
      HctPrms.regionName);

  protected final String deltaType = TestConfig.tab().stringAt(
      DeltaPropagationPrms.objectType, "delta.DeltaTestObj");

  /** Total number of regions in the tests. Default is one */
  static int numOfRegion = 1;

  /**
   * The last key to be put by feeder. This is used to signal for proceeding
   * with the validation in some tests
   */
  public static final String LAST_KEY = "last_key";

  public static boolean isClient = false;
  
  public static HashMap keyToMap = new HashMap();

  /**
   * Randomly pick a key within the PUT_KEY_RANGE for doing operation.
   * 
   * @return string key
   */
  private static String getKeyForOperation() {
    // random number between 0 AND PUT_KEY_RANGE
    Random r = new Random();
    int randint = r.nextInt(PUT_KEY_RANGE);

    return ("KEY-" + randint);
  }

  /**
   * Perform create operation for a random key within PUT_KEY_RANGE
   * 
   * @param region -
   *          the test region
   */
  protected static void createEntry(Region region) {
    String key = getKeyForOperation();
    try {
      if (!region.containsKey(key)) {
        region.create(key, key + "_VALUE");
      }
    }
    catch (EntryExistsException e) {
      Log.getLogWriter().warning(
          "EntryExistsException occured while creating entry. "
              + TestHelper.getStackTrace(e));
    }
    catch (Exception e) {
      exceptionMsg.append("Exception occured while creating entry. "
          + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }
  }

  /**
   * Perform put operation for a random key within PUT_KEY_RANGE
   * 
   * @param region -
   *          the test region
   */
  protected static void updateEntry(Region region) {
    String key = getKeyForOperation();
    try {
      if (region.containsKey(key)) {
        region.put(key, key + "_VALUE");
      }
    }
    catch (Exception e) {
      exceptionMsg.append("Exception occured while updating entry. "
          + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }
  }

  /**
   * Perform invalidate operation for a random key within PUT_KEY_RANGE
   * 
   * @param region -
   *          the test region
   */
  protected static void invalidateEntry(Region region) {
    String key = getKeyForOperation();
    try {
      if (region.containsKey(key) && (region.get(key) != null)) {
        region.invalidate(key);
      }
    }
    catch (EntryNotFoundException e) {
      Log.getLogWriter().warning(
          "EntryNotFoundException occured while invalidating entry. "
              + TestHelper.getStackTrace(e));
    }
    catch (Exception e) {
      exceptionMsg.append("Exception occured while invalidating entry. "
          + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }
  }

  /**
   * Perform destroy operation for a random key within PUT_KEY_RANGE
   * 
   * @param region -
   *          the test region
   */
  protected static void destroyEntry(Region region) {
    String key = getKeyForOperation();
    try {
      if (region.containsKey(key)) {
        region.destroy(key);
      }
    }
    catch (EntryNotFoundException e) {
      Log.getLogWriter().warning(
          "EntryNotFoundException occured while invalidating entry. "
              + TestHelper.getStackTrace(e));
    }
    catch (Exception e) {
      exceptionMsg.append("Exception occured while invalidating entry. "
          + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }
  }

  /**
   * Perform registerInterest operation for a random key within PUT_KEY_RANGE
   * 
   * @param region -
   *          the test region
   */
  protected static void registerInterest(Region region) {
    String key = getKeyForOperation();
    long start = System.currentTimeMillis();
    long total = 0;
    try {

      region.registerInterest(key);
      total = System.currentTimeMillis() - start;
    }
    catch (Exception e) {
      exceptionMsg.append("Exception occured while registerInterest. "
          + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }
    if (total > 0) {
      Log.getLogWriter().info("time taken for registerInterest : " + total);
    }
  }

  /**
   * Perform unregisterInterest operation for a random key within PUT_KEY_RANGE
   * 
   * @param region -
   *          the test region
   */
  protected static void unregisterInterest(Region region) {
    String key = getKeyForOperation();
    long start = System.currentTimeMillis();
    long total = 0;
    try {
      region.unregisterInterest(key);
      total = System.currentTimeMillis() - start;
    }
    catch (Exception e) {
      exceptionMsg.append("Exception occured while unregisterInterest. "
          + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }
    if (total > 0) {
      Log.getLogWriter().info("time taken for unregisterInterest : " + total);
    }
  }

  /**
   * Does {@link #feederTask} for {@link DeltaPropagationPrms#feederTaskTimeSec}.
   */
  public static void feederTimedTask() {

    long end = System.currentTimeMillis()
        + TestConfig.tab().longAt(DeltaPropagationPrms.feederTaskTimeSec)
        * 1000;
    do {
      feederTask();
    } while (System.currentTimeMillis() < end);
  }

  /**
   * Increments the value of counters in <code>threadList</code> for each
   * thread and notifies all the put threads int wait().
   * 
   */
  public static void feederTask() {
    synchronized (threadList) {
      String exceptionReason = Validator.checkBlackBoardForException();
      // This check is for exception occued during previous feeder operation.
      if (exceptionOccured)
        throw new TestException(exceptionMsg.toString());
      // This check is for the exception occred in the edges.
      if (exceptionReason != null) {
        stop = true;
        threadList.notifyAll();
        for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
          try {
            putThreads[i].join(120000);
          }
          catch (InterruptedException e) {
            throw new TestException(e.getMessage()
                + TestHelper.getStackTrace(e));
          }
        }
        throw new TestException(exceptionReason);
      }
      // If there is no exception then we can start new Feeder Task
      else {
        for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
          int newCounter = ((Integer)threadList.get(i)).intValue() + 1;
          threadList.set(i, new Integer(newCounter));
        }
        threadList.notifyAll();
      }
    }
  }

  /*  private static int counter = 0;*/

  public static void populateSharedMapWithRegionData() {
    try {
      numOfRegion = TestConfig.tab().intAt(
          delta.DeltaPropagationPrms.numberOfRegions, 1);
      for (int i = 0; i < numOfRegion; i++) {
        Region region = RegionHelper.getRegion(regionName + i);

        if (region == null) {
          throw new TestException("Region created is null "
              + TestHelper.getStackTrace());
        }

        if (region.isEmpty()) {
          throw new TestException(
              " Region has no entries to copy to the SharedMap "
                  + TestHelper.getStackTrace());
        }

        Iterator iterator = region.entrySet(false).iterator();
        Region.Entry entry = null;
        Object key;
        Object value;
        while (iterator.hasNext()) {
          entry = (Region.Entry)iterator.next();
          key = entry.getKey();
          value = entry.getValue();
          if (value != null) {
            DeltaPropagationBB.getBB().getSharedMap().put(key, value);
          }
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new TestException(e.getMessage() + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Signals for stopping of the put threads and notifies all. Waits till all
   * the put threads are complete and decrements the shared counter for
   * TOTAL_UPDATES by <code>TOTAL_PUT_THREADS * PUT_KEY_RANGE</code>. This is
   * done because the first set of PUTs by each thread will correspond to CREATE
   * operations and UPDATES afterwards.
   */
  public static void closeTask() {

    waitForFeederThreadsToComplete();
    Region region = RegionHelper.getRegion(regionName + 0);

    if (region == null) {
      throw new TestException("Region created is null "
          + TestHelper.getStackTrace());
    }

    try {
      Thread.sleep(15000);
    }
    catch (InterruptedException e1) {

      e1.printStackTrace();
    }

    // for to keep track of todeltas called
    for (Iterator i = keyToMap.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      DeltaTestObj obj = (DeltaTestObj)keyToMap.get(key);
      DeltaPropagationBB.getBB().getSharedMap().put(key,
          obj.getToDeltaCounter());
    }
    
    try {
      Long last_value = new Long(0);
      region.put(LAST_KEY, last_value);
      Log.getLogWriter().info(
          "Putting the last key in the region: " + region.getName());

      DeltaPropagationBB.getBB().getSharedCounters().add(
          DeltaPropagationBB.NUM_CREATE, 1);
    }
    catch (Exception e) {
      throw new TestException("Exception while performing put of the last_key"
          + TestHelper.getStackTrace());
    }

    Log.getLogWriter().info(
        "No of puts done by feeder : "
            + (DeltaPropagationBB.getBB().getSharedCounters().read(
                DeltaPropagationBB.NUM_UPDATE)
                + DeltaPropagationBB.getBB().getSharedCounters().read(
                    DeltaPropagationBB.NUM_DELTA_UPDATE) + DeltaPropagationBB
                .getBB().getSharedCounters().read(
                    DeltaPropagationBB.NUM_NON_DELTA_UPDATE)));

    Log.getLogWriter().info("Close Task Complete...");
  }

  public static void verifyDeltaFailuresInC2S() {
    CachePerfStats stats = ((DistributedRegion)GemFireCacheImpl.getInstance()
        .getRegion(regionName + 0)).getCachePerfStats();
    if (stats.getDeltaFailedUpdates() == 0) {
      throw new TestException(
          "CachePerfStats indicating failed delta updates is expected to be non-zero.");
    }
  }

  public static void verifyDeltaFailuresInP2P() {
    CachePerfStats stats = ((DistributedRegion)GemFireCacheImpl.getInstance()
        .getRegion(regionName + 0)).getCachePerfStats();
    if (stats.getDeltaFailedUpdates() == 0) {
      throw new TestException(
          "CachePerfStats indicating failed delta updates is expected to be non-zero.");
    }
  }

  public static void verifyDeltaFailuresInS2C() {
    // TODO: (ashetkar) Get hold of CCUStats
    throw new TestException(
        "CachePerfStats indicating failed delta updates is expected to be non-zero.");
  }

  /**
   * Signals for stopping of the put threads and notifies all. Waits till all
   * the put threads are complete. Signals to other VMs that the feed cyle is
   * over.
   */
  public static void waitForFeederThreadsToComplete() {
    synchronized (threadList) {
      stop = true;
      threadList.notifyAll();
    }

    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      try {
        putThreads[i].join(120000);
      }
      catch (InterruptedException e) {
        throw new TestException(e.getMessage() + TestHelper.getStackTrace(e));
      }
    }

    // fail the test if exception occured in any of the put threads
    if (exceptionOccured) {
      throw new TestException(exceptionMsg.toString());
    }

    // signal other VMs by making the feed signal counter non-zero
    Log.getLogWriter().info("Setting feed signal...");
    try {
      DeltaPropagationBB.getBB().getSharedCounters().increment(
          DeltaPropagationBB.feedSignal);
    }
    catch (Exception e) {
      Log.getLogWriter()
          .info("Exception occured while updating SharedCounter ");
      exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
      exceptionOccured = true;
    }

    Validator.checkBlackBoardForException();
  }

  /**
   * Gets the latest value for the given key from the latestValues map and
   * returns the incremented value for doing entry operations.
   * 
   * @param key -
   *          the event key
   * @param threadIdKey -
   *          thread-id to which this key belongs
   * @return - the latest incremented value for this key
   */
  protected static Long getLatestValue(String key, String threadIdKey) {
    Map latestValueForThread = (Map)latestValues.get(threadIdKey);
    Long oldVal = (Long)latestValueForThread.remove(key);
    if (oldVal == null) {
      throw new TestException("oldVal cannot be null");
    }
    return new Long(oldVal.longValue() + 1);
  }
  
  /**
   * Gets the latest delta for the given key from the latestValues map and
   * returns the incremented value for doing entry operations.
   * 
   * @param key -
   *          the event key
   * @param threadIdKey -
   *          thread-id to which this key belongs
   * @param isCreate - create or update operation
   * @return - the latest incremented delta for this key
   */
  protected static DeltaTestObj getLatestDelta(String key, String threadIdKey,
      boolean isCreate) {
    Map latestValueForThread = (Map)latestValues.get(threadIdKey);
    DeltaTestObj oldVal = (DeltaTestObj)latestValueForThread.remove(key);
    if (oldVal == null) {
      throw new TestException("oldDelta cannot be null");
    }
    DeltaTestObj obj = null;
    obj = new DeltaTestObj(oldVal.getIntVar() + 1, "delta");
    if (!isCreate) {
      obj.setIntVar(oldVal.getIntVar() + 1);
    }
    return obj;
  }

  /**
   * Updates the local <code>latestValues</code> map on a invalidate or
   * destroy call.
   * 
   * @param key -
   *          the event key
   * @param threadId -
   *          thread-id to which this key belongs
   * @param isDestroyCall -
   *          true if this method was called for doing destroy operation, false
   *          if invalidate operation
   * @return - whether to proceed with the calling invalidate or destroy
   *         operation or not.
   * @throws Exception -
   *           thrown if any exception occurs while fetching the old value from
   *           the region
   */
  protected static boolean updateLatestValueMap(Region region, String key,
      String threadId, boolean isDestroyCall) throws Exception {
    Object oldVal = null;
    if (region.containsKey(key)) {
      if ((oldVal = region.get(key)) == null) {
        if (isDestroyCall) {
          return true;
        }
        else {
          return false;
        }
      }
      else {
        Map latestValueForThread = (Map)latestValues.get(threadId);
        latestValueForThread.put(key, oldVal);
        return true;
      }
    }
    else {
      return false;
    }
  }

  /**
   * This method is an INITTASK and does the following : <br>
   * 1)Create cache adn region <br>
   * 2)Set the counters for all threads to 1 ( to run them once in INITTASK)<br>
   * 3)Initialize and start the put threads
   * 4)Put object of Delta or Byte[] type
   * @since 6.1
   */
  public static void initToFeedMixOfDeltaAndOthers() {
    if (!isClient) {
      CacheHelper.createCache(ConfigPrms.getCacheConfig());
      numOfRegion = TestConfig.tab().intAt(
          delta.DeltaPropagationPrms.numberOfRegions, 1);
      for (int i = 0; i < numOfRegion; i++) {
        Region region = RegionHelper.createRegion(regionName + i, ConfigPrms
            .getRegionConfig());

        if (region == null) {
          exceptionMsg.append("Region created is null ");
          exceptionOccured = true;
        }
        String threadIdkeyPrefix = "Thread_";
        createMixOfDeltaEntries(region, threadIdkeyPrefix);
      }
    }
    else {
      DeltaPropagation.initCacheClient();
      String regionName = TestConfig.tab().stringAt(HctPrms.regionName);
      final Region region = RegionHelper.getRegion(Region.SEPARATOR
          + regionName + 0);
      if (region == null) {
        exceptionMsg.append("Region created is null ");
        exceptionOccured = true;
      }
      String threadIdkeyPrefix = "Thread_";
      createMixOfDeltaEntries(region, threadIdkeyPrefix);
    }

    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      threadList.add(new Integer(0));
    }

    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      final int threadId = i;
      putThreads[i] = new Thread() {

        /**
         * Each thread checks the value of its correponding counter from
         * <code>threadList</code>. If the value is zero, then goes into
         * wait(). If the value is greater than zero,than decrements it by one
         * and performs put/invalidate/destroy operation on its fixed set of
         * keys and updates the blackboard for total number of
         * creates/puts/invalidates/destroys done. If in wait(), this will be
         * notified via the hydra TASK.
         */
        public void run() {
          while (true) {
            synchronized (threadList) {
              if ((((Integer)threadList.get(threadId)).intValue()) == 0
                  && !stop) {
                try {
                  threadList.wait();
                }
                catch (InterruptedException e) {
                  exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
                  exceptionOccured = true;
                }
              }
              //It may possible that the stop became true during the FeederTask. So check is added here.
              if (stop)
                break;
              threadList.set(threadId, new Integer(((Integer)threadList
                  .get(threadId)).intValue() - 1));
            }

            String threadIdkey = "Thread_" + threadId;

            // if entries not created yet, throw exception.
            if (latestValues.get(threadIdkey) == null) {
              TestException ex = new TestException(
                  "Entries were not created in INITTASK for keys containing threadIdkey = "
                      + threadIdkey);
              exceptionMsg.append(TestHelper.getStackTrace(ex));
              exceptionOccured = true;
            }
            else {
              // entries already created once, do random operations
              int numNonDeltaCreate = 0;
              int numNonDeltaUpdate = 0;
              int numDeltaCreate = 0;
              int numDeltaUpdate = 0;
              int numInvalidate = 0;
              int numDestroy = 0;

              Region region = RegionHelper.getRegion(Region.SEPARATOR
                  + regionName + 0);
              for (int j = 0; j < PUT_KEY_RANGE; j++) {
                try {
                  String key = region.getFullPath() + threadIdkey + "_" + j;
                  String operation = TestConfig.tab().stringAt(
                      DeltaPropagationPrms.entryOperations, "put");
                  
                  if (j % 2 == 0)
                    key = key + "_n_d_o";
                  
                  if (operation.equals("put")) {
                    if (j % 2 == 1) {
                      DeltaTestObj newVal = null;
                      if (region.containsKey(key)) {
                        DeltaTestObj oldVal = (DeltaTestObj)region.get(key);
                        if (oldVal == null) {
                          newVal = getLatestDelta(key, threadIdkey, false);
                          region.put(key, newVal);
                        }
                        else {
                          newVal = oldVal;
                          newVal.setIntVar(oldVal.getIntVar() + 1);
                          Log.getLogWriter().fine("update value : " + newVal);
                          region.put(key, newVal);

                        }
                        numDeltaUpdate++;
                      }
                      else {
                        newVal = getLatestDelta(key, threadIdkey, true);
                        Log.getLogWriter().fine("create value : " + newVal);
                        region.create(key, newVal);
                        numDeltaCreate++;
                      }
                      Log.getLogWriter().fine(
                          "puting entry in Map key " + key + " : value : "
                              + newVal);
                      /*
                       * DeltaPropagationBB.getBB().getSharedMap().getMap().put(key,
                       * newVal);
                       */
                      keyToMap.put(key, newVal);
                    }
                    else {
                      Long newVal = null;
                      if (region.containsKey(key)) {
                        Long oldVal = (Long)region.get(key);
                        if (oldVal == null) {
                          newVal = getLatestValue(key, threadIdkey);
                          region.put(key, newVal);
                        }
                        else {
                          newVal = oldVal.longValue() + 1;
                          Log.getLogWriter().fine("update value : " + newVal);
                          region.put(key, newVal);
                        }
                        numNonDeltaUpdate++;
                      }
                      else {
                        newVal = getLatestValue(key, threadIdkey);
                        Log.getLogWriter().fine("create value : " + newVal);
                        region.create(key, newVal);
                        numNonDeltaCreate++;
                      }
                    }
                  }
                  else if (operation.equals("destroy")) {
                    if (updateLatestValueMap(region, key, threadIdkey, true)) {
                      region.destroy(key);
                      numDestroy++;
                      Log.getLogWriter().fine("remove/destroy entry in Map key " +key);
                      keyToMap.remove(key); // if present
                    }
                  }
                  else {
                    throw new TestException("Unknown entry operation: "
                        + operation);
                  }
                }
                catch (Exception e) {
                  Log.getLogWriter().info(
                      TOTAL_PUT_THREADS
                          + "exception in put thread starting with " + threadId
                          + " for key = " + j);
                  exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
                  exceptionOccured = true;

                }
              }

              // update the blackboard

              try {
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_NON_DELTA_CREATE, numNonDeltaCreate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_NON_DELTA_UPDATE, numNonDeltaUpdate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_INVALIDATE, numInvalidate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_DESTROY, numDestroy);
                // counter to keep of Delta
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_DELTA_CREATE, numDeltaCreate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_DELTA_UPDATE, numDeltaUpdate);
              }
              catch (Exception e) {
                Log.getLogWriter().info(
                    "Exception occured while updating SharedCounter ");
                exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
                exceptionOccured = true;
              }

              Log.getLogWriter().info(
                  "Task completed for thread id = " + threadId);
            }
            if (stop)
              break;
          }
        }
      };
      putThreads[i].start();
    }
  }

  /**
   * This method is an INITTASK and does the following : <br>
   * 1)Create cache adn region <br>
   * 2)Set the counters for all threads to 1 ( to run them once in INITTASK)<br>
   * 3)Initialize and start the put threads
   * 
   */
  public static void initTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());

    numOfRegion = TestConfig.tab().intAt(
        delta.DeltaPropagationPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = RegionHelper.createRegion(regionName + i, ConfigPrms
          .getRegionConfig());

      if (region == null) {
        exceptionMsg.append("Region created is null");
        exceptionOccured = true;
      }
      String threadIdkeyPrefix = "Thread_";
      createDeltaEntries(region, threadIdkeyPrefix);
    }

    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      threadList.add(new Integer(0));
    }
    spawnFeederThreads();
  }

  /**
   * This method is an INITTASK and does the following : <br>
   * 1)Create cache adn region <br>
   * 
   */
  public static void feederCreateRegions() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());

    numOfRegion = TestConfig.tab().intAt(
        delta.DeltaPropagationPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = RegionHelper.createRegion(regionName + i, ConfigPrms
          .getRegionConfig());

      if (region == null) {
        exceptionMsg.append("Region created is null");
        exceptionOccured = true;
      }
    }
  }

  /**
   * This method is an INITTASK and does the following : <br>
   * 2)Set the counters for all threads to 1 ( to run them once in INITTASK)<br>
   * 3)Initialize and start the put threads
   * 
   */
  public static void feederSpawnThreads() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());

    numOfRegion = TestConfig.tab().intAt(
        delta.DeltaPropagationPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = RegionHelper.getRegion(regionName + i);
      String threadIdkeyPrefix = "Thread_";
      createDeltaEntries(region, threadIdkeyPrefix);
    }

    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      threadList.add(new Integer(0));
    }
    spawnFeederThreads();
  }

  public static void initTaskOnBridge() {
    numOfRegion = TestConfig.tab().intAt(
        delta.DeltaPropagationPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = GemFireCacheImpl.getInstance().getRegion(regionName + i);
      String threadIdkeyPrefix = "Thread_";
      createDeltaEntries(region, threadIdkeyPrefix);
    }
    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      threadList.add(new Integer(0));
    }
    spawnFeederThreads();
  }

  public static void doNothing() {
  }

  private static void spawnFeederThreads() {
    for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
      final int threadId = i;
      putThreads[i] = new Thread() {

        /**
         * Each thread checks the value of its correponding counter from
         * <code>threadList</code>. If the value is zero, then goes into
         * wait(). If the value is greater than zero,than decrements it by one
         * and performs put/invalidate/destroy operation on its fixed set of
         * keys and updates the blackboard for total number of
         * creates/puts/invalidates/destroys done. If in wait(), this will be
         * notified via the hydra TASK.
         */
        public void run() {
          while (true) {
            synchronized (threadList) {
              if ((((Integer)threadList.get(threadId)).intValue()) == 0
                  && !stop) {
                try {
                  threadList.wait();
                }
                catch (InterruptedException e) {
                  exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
                  exceptionOccured = true;
                }
              }
              // It may possible that the stop became true during the
              // FeederTask. So check is added here.
              if (stop) {
                break;
              }
              threadList.set(threadId, new Integer(((Integer)threadList
                  .get(threadId)).intValue() - 1));
            }

            String threadIdkey = "Thread_" + threadId;

            // if entries not created yet, throw exception.
            if (latestValues.get(threadIdkey) == null) {
              TestException ex = new TestException(
                  "Entries were not created in INITTASK for keys containing threadIdkey = "
                      + threadIdkey);
              exceptionMsg.append(TestHelper.getStackTrace(ex));
              exceptionOccured = true;
            }
            else {
              // entries already created once, do random operations
              int numCreate = 0;
              int numUpdate = 0;
              int numInvalidate = 0;
              int numDestroy = 0;

              // Select the region randomly and perform operations on it
              int numRegion = TestConfig.tab().intAt(
                  DeltaPropagationPrms.regionRange, 1) - 1;
              Region region = RegionHelper.getRegion(regionName + numRegion);
              for (int j = 0; j < PUT_KEY_RANGE; j++) {
                try {
                  String key = region.getFullPath() + threadIdkey + "_" + j;
                  String operation = TestConfig.tab().stringAt(
                      DeltaPropagationPrms.entryOperations, "put");

                  if (operation.equals("put")) {
                    DeltaTestObj newVal = null;
                    if (region.containsKey(key)) {
                      DeltaTestObj oldVal = (DeltaTestObj)region.get(key);
                      if (oldVal == null) {
                        newVal = getLatestDelta(key, threadIdkey, false);
                        region.put(key, newVal);
                      }
                      else {
                        newVal = new DeltaTestObj(oldVal);
                        newVal.setIntVar(oldVal.getIntVar() + 1);
                        Log.getLogWriter().info("update value: " + newVal);
                        if (region instanceof PartitionedRegion) {// for accessor
                          if (((PartitionedRegion)region).getLocalMaxMemory() == 0) {
                            // set the toDeltaCounter to old Value toDeltaCounter
                            // as get will be remote get for PR accessor resets
                            // toDeltaCounter
                            newVal.setToDeltaCounter(newVal.getIntVar() - 1);
                          }
                        }
                        region.put(key, newVal);
                      }
                      numUpdate++;
                    }
                    else {
                      newVal = getLatestDelta(key, threadIdkey, true);
                      Log.getLogWriter().info("create value: " + newVal);
                      region.create(key, newVal);
                      numCreate++;
                    }
                    Log.getLogWriter().info(
                        "Put entry in Map. key: " + key + ", value: "
                            + newVal);
                    /*DeltaPropagationBB.getBB().getSharedMap().getMap().put(key, newVal);*/
                    keyToMap.put(key, newVal);
                  }
                  else if (operation.equals("invalidate")) {
                    if (updateLatestValueMap(region, key, threadIdkey, false)) {
                      region.invalidate(key);
                      numInvalidate++;
                      Log.getLogWriter().info(
                          "Invalidated entry in Map. key: " + key + ", value: "
                              + null);
                      /*DeltaPropagationBB.getBB().getSharedMap().getMap().put(key, null);*/
                      keyToMap.put(key, null);
                    }
                  }
                  else if (operation.equals("destroy")) {
                    if (updateLatestValueMap(region, key, threadIdkey, true)) {
                      region.destroy(key);
                      numDestroy++;
                      Log.getLogWriter().info(
                          "Destroyed entry in Map. key: " + key);
                      /*DeltaPropagationBB.getBB().getSharedMap().getMap().remove(key);*/
                      keyToMap.remove(key);
                    }
                  }
                  else {
                    throw new TestException("Unknown entry operation: "
                        + operation);
                  }
                }
                catch (Exception e) {
                  Log.getLogWriter().info(
                      TOTAL_PUT_THREADS
                          + "exception in put thread starting with " + threadId
                          + " for key = " + j);
                  exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
                  exceptionOccured = true;
                }
              }

              // update the blackboard
              try {
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_CREATE, numCreate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_UPDATE, numUpdate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_INVALIDATE, numInvalidate);
                DeltaPropagationBB.getBB().getSharedCounters().add(
                    DeltaPropagationBB.NUM_DESTROY, numDestroy);
              }
              catch (Exception e) {
                Log.getLogWriter().info(
                    "Exception occured while updating SharedCounter");
                exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
                exceptionOccured = true;
              }

              Log.getLogWriter().info(
                  "Task completed for thread id = " + threadId);
            }
            if (stop)
              break;
          }
        }
      };
      putThreads[i].start();
    }
  }

  /**
   * Create all the delta test entries in the given region.
   * 
   * @param region -
   *          The region in which entries will be created
   * @param threadIdKeyPrefix -
   *          The string prefix used to generate keys
   */
  private static void createDeltaEntries(Region region, String threadIdKeyPrefix) {
    // put data in region
    for (int j = 0; j < TOTAL_PUT_THREADS; j++) {
      String threadIdkey = threadIdKeyPrefix + j;
      Map values = new HashMap();      
      for (int k = 0; k < PUT_KEY_RANGE; k++) {
        String key = region.getFullPath() + threadIdkey + "_" + k;
        try {
          DeltaTestObj initValue = new DeltaTestObj(0, "delta");
          region.create(key, initValue);
          values.put(key, initValue);
          keyToMap.put(key, initValue);
        }
        catch (Exception e) {
          Log.getLogWriter().info(
              TOTAL_PUT_THREADS
                  + "Exception in creating entry for starting with " + j
                  + " for key = " + key);
          exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
          exceptionOccured = true;
        }
        /*DeltaPropagationBB.getBB().getSharedMap().getMap().put(key, initValue);*/
      }
      latestValues.put(threadIdkey, values);

      try {
        DeltaPropagationBB.getBB().getSharedCounters().add(
            DeltaPropagationBB.NUM_CREATE, PUT_KEY_RANGE);
      }
      catch (Exception e) {
        Log.getLogWriter().info(
            "Exception occured while updating SharedCounter ");
        exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
        exceptionOccured = true;
      }
    }
    Log.getLogWriter().info(
        "Created entries on region : " + region.getFullPath());
  }

  /**
   * Create all the test entries(mixture of Delta and other)in the given region.
   * 
   * @param region -
   *          The region in which entries will be created
   * @param threadIdKeyPrefix -
   *          The string prefix used to generate keys
   * @since 6.1
   * 
   */
  private static void createMixOfDeltaEntries(Region region, String threadIdKeyPrefix) {
    // put data in region
    for (int j = 0; j < TOTAL_PUT_THREADS; j++) {
      String threadIdkey = threadIdKeyPrefix + j;
      Map values = new HashMap();
      for (int k = 0; k < PUT_KEY_RANGE; k++) {
        String key = region.getFullPath() + threadIdkey + "_" + k;
        if (k % 2 == 1) { // create delta
          DeltaTestObj newVal = new DeltaTestObj(0, "delta");
          try {
            region.create(key, newVal);
          }
          catch (Exception e) {
            Log.getLogWriter().info(
                TOTAL_PUT_THREADS
                    + "Exception in creating entry for starting with " + j
                    + " for key = " + key);
            exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
            exceptionOccured = true;
          }
          values.put(key, newVal);
          keyToMap.put(key, newVal);
          try {
            DeltaPropagationBB.getBB().getSharedCounters().add(
                DeltaPropagationBB.NUM_DELTA_CREATE, 1);
          }
          catch (Exception e) {
            Log.getLogWriter().info(
                "Exception occured while updating SharedCounter ");
            exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
            exceptionOccured = true;
          }
        }
        else {
          Long initValue = new Long(0);
          key = key + "_n_d_o";
          try {
            region.create(key, initValue);
          }
          catch (Exception e) {
            Log.getLogWriter().info(
                TOTAL_PUT_THREADS
                    + "Exception in creating entry for starting with " + j
                    + " for key = " + key);
            exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
            exceptionOccured = true;
          }
          values.put(key, initValue);
          try {
            DeltaPropagationBB.getBB().getSharedCounters().add(
                DeltaPropagationBB.NUM_NON_DELTA_CREATE, 1);
          }
          catch (Exception e) {
            Log.getLogWriter().info(
                "Exception occured while updating SharedCounter ");
            exceptionMsg.append(e.getMessage() + " " + TestHelper.getStackTrace(e));
            exceptionOccured = true;
          }
        }
      }
      latestValues.put(threadIdkey, values);
    }
    Log.getLogWriter().info(
        "Created entries on region: " + region.getFullPath());
  }

  public static void feederIsClient() {
    isClient = true;
  }
  
  public static void noResetTask() {
    DeltaTestObj.NEED_TO_RESET_T0_DELTA = false;
  }
}
