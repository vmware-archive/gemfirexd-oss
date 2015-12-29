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
package durableClients;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

import com.gemstone.gemfire.cache.*;

import util.TestException;
import util.TestHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

/**
 * @author Aneesh Karayil
 * @since 5.2
 */
public class Feeder {
  /** Total number of perpetual put threads to be created */
  final static int TOTAL_PUT_THREADS = TestConfig.tab().intAt(
      DurableClientsPrms.numPutThreads);

  /** range of keys to be used by each thread */
  final static int PUT_KEY_RANGE = TestConfig.tab().intAt(
      DurableClientsPrms.numKeyRangePerThread);

  /** A static array containing the reference to the put threads */
  static Thread[] putThreads = new Thread[TOTAL_PUT_THREADS];

  /** A regionName to which index is attached to create multiple regions */
  final static String regionName = TestConfig.tab().stringAt(
      DurableClientsPrms.regionName);

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

  public static ArrayList keyList = new ArrayList();

  public static ArrayList partialList = new ArrayList();

  // test region name
  static protected final String REGION_NAME = TestConfig.tab().stringAt(
      DurableClientsPrms.regionName, "Region");

  /** Total number of regions in the tests. Default is one */
  static int numOfRegion = 1;

  /**
   * The last key to be put by feeder. This is used to signal for proceeding
   * with the validation in some tests
   */
  public static final String LAST_KEY = "last_key";

  /**
   * Create all the test entries in the given region.
   * 
   * @param region -
   *          The region in which entries will be created
   * @param threadIdKeyPrefix -
   *          The string prefix used to generate keys
   */
  private static void createEntries(Region region, String threadIdKeyPrefix) {
    // put data in region
    for (int j = 0; j < TOTAL_PUT_THREADS; j++) {
      String threadIdkey = threadIdKeyPrefix + j;
      Map values = new HashMap();
      Long initValue = new Long(0);
      for (int k = 0; k < PUT_KEY_RANGE; k++) {
        String key = region.getFullPath() + threadIdkey + "_" + k;
        //String key = threadIdkey + "_" + k;
        try {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
            Log.getLogWriter().fine("putIfAbsent(" + key + ")");
            region.putIfAbsent(key, initValue);
          } else {
            Log.getLogWriter().fine("create(" + key + ")");
          region.create(key, initValue);
          }
          Log.getLogWriter().info(
              "Adding key to the Full ArrayList " + Feeder.keyList.add(key)
                  + " Size is " + keyList.size());
          if (k % 2 == 0) {
            Log.getLogWriter().info(
                "Adding key to the Partial ArrayList "
                    + Feeder.partialList.add(key) + "Size is "
                    + partialList.size());
          }
        }
        catch (Exception e) {
          Log.getLogWriter().info(
              TOTAL_PUT_THREADS
                  + "Exception in creating entry for starting with " + j
                  + " for key = " + key);
          exceptionMsg.append(e.getMessage());
          exceptionOccured = true;
        }
        values.put(key, initValue);
      }
      latestValues.put(threadIdkey, values);
      durableClients.DurableClientsBB.getBB().getSharedMap().put(
          "FULL LIST : ", keyList);
      durableClients.DurableClientsBB.getBB().getSharedMap().put(
          "PARTIAL LIST : ", partialList);
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
        durableClients.DurableClientsPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = RegionHelper.createRegion(regionName + i, ConfigPrms
          .getRegionConfig());

      if (region == null) {
        exceptionMsg.append("Region created is null ");
        exceptionOccured = true;
      }
      String threadIdkeyPrefix = "Thread_";
      Feeder.createEntries(region, threadIdkeyPrefix);
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
                  exceptionMsg.append(e.getMessage());
                  exceptionOccured = true;
                }
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
                  DurableClientsPrms.regionRange, 1) - 1;
              Region region = RegionHelper.getRegion(regionName + numRegion);
              for (int j = 0; j < PUT_KEY_RANGE; j++) {
                try {
                  String key = region.getFullPath() + threadIdkey + "_" + j;
                  String operation = TestConfig.tab().stringAt(
                      DurableClientsPrms.entryOperations, "put");

                  if (operation.equals("put")) {
                    Long newVal = null;
                    if (region.containsKey(key)) {
                      Long oldVal = (Long)region.get(key);
                      if (oldVal == null) {
                        newVal = getLatestValue(key, threadIdkey);
                        if (TestConfig.tab().getRandGen().nextBoolean()) {
                          Log.getLogWriter().fine("replace(" + key + ")");
                          region.replace(key, newVal);
                        } else {
                          Log.getLogWriter().fine("put(" + key + ")");
                        region.put(key, newVal);
                      }
                      }
                      else {
                        newVal = new Long(oldVal.longValue() + 1);
                        if (TestConfig.tab().getRandGen().nextBoolean()) {
                          Log.getLogWriter().fine("replace(" + key + ")");
                          region.replace(key, newVal);
                        } else {
                          Log.getLogWriter().fine("put(" + key + ")");
                        region.put(key, newVal);
                      }
                      }
                      numUpdate++;
                    }
                    else {
                      newVal = getLatestValue(key, threadIdkey);
                      if (TestConfig.tab().getRandGen().nextBoolean()) {
                        Log.getLogWriter().fine("putIfAbsent(" + key + ")");
                        region.putIfAbsent(key, newVal);
                      } else {
                        Log.getLogWriter().fine("create(" + key + ")");
                      region.create(key, newVal);
                      }
                      numCreate++;
                    }
                  }

                  else if (operation.equals("invalidate")) {
                    if (updateLatestValueMap(region, key, threadIdkey, false)) {
                      region.invalidate(key);
                      numInvalidate++;
                    }
                  }
                  else if (operation.equals("destroy")) {
                    if (updateLatestValueMap(region, key, threadIdkey, true)) {
                      if (TestConfig.tab().getRandGen().nextBoolean()) {
                        Log.getLogWriter().fine("remove(" + key + ")");
                        region.remove(key, region.get(key));
                      } else {
                        Log.getLogWriter().fine("destroy(" + key + ")");
                      region.destroy(key);
                      }
                      numDestroy++;
                    }
                  }
                  else {
                    throw new TestException("Unknown entry operation: "
                        + operation);
                  }
                  Thread.sleep(10);
                }
                catch (Exception e) {
                  Log.getLogWriter().info(
                      TOTAL_PUT_THREADS
                          + "exception in put thread starting with " + threadId
                          + " for key = " + j);
                  exceptionMsg.append(e.getMessage());
                  exceptionOccured = true;

                }
              }

              // update the blackboard

              Log.getLogWriter().info(
                  "Task completed thread for id = " + threadId);
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
        if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.putIfAbsent(key, key + "_VALUE");
        } else {
        region.create(key, key + "_VALUE");
      }
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
        if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.replace(key, key + "_VALUE");
        } else {
        region.put(key, key + "_VALUE");
      }
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
        if (TestConfig.tab().getRandGen().nextBoolean()) {
          region.remove(key, region.get(key));
        } else {
        region.destroy(key);
      }
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
   * Does {@link #feederTask} for {@link HAClientQueuePrms#feederTaskTimeSec}.
   */
  /*
   * public static void feederTimedTask() {
   * 
   * long end = System.currentTimeMillis() +
   * TestConfig.tab().longAt(HAClientQueuePrms.feederTaskTimeSec) * 1000; do {
   * feederTask(); } while (System.currentTimeMillis() < end); }
   */
  /**
   * Increments the value of counters in <code>threadList</code> for each
   * thread and notifies all the put threads int wait().
   * 
   */
  public static void feederTask() {
    synchronized (threadList) {
      DurableClientsTest.checkBlackBoardForException();
      for (int i = 0; i < TOTAL_PUT_THREADS; i++) {
        int newCounter = ((Integer)threadList.get(i)).intValue() + 1;
        threadList.set(i, new Integer(newCounter));
      }
      threadList.notifyAll();
    }
  }

//  private static int counter = 0;

  public static void populateSharedMapWithRegionData() {
    try {
      numOfRegion = TestConfig.tab().intAt(DurableClientsPrms.numberOfRegions,
          1);
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
            DurableClientsBB.getBB().getSharedMap().put(key, value);
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

    if (TestConfig.tab().booleanAt(
        durableClients.DurableClientsPrms.putLastKey, false)) {

      try {
        Thread.sleep(15000);
      }
      catch (InterruptedException e1) {
        e1.printStackTrace();
      }

      try {
        Long last_value = new Long(0);
        region.put(LAST_KEY, last_value);
        Log.getLogWriter().info(
            "Putting the last key in the region: " + region.getName());
      }
      catch (Exception e) {
        throw new TestException(
            "Exception while performing put of the last_key"
                + TestHelper.getStackTrace());
      }
    }

    Log.getLogWriter().info("Close Task Complete...");
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
      // HAClientQueueBB.getBB().getSharedCounters()
      // .increment(HAClientQueueBB.feedSignal);
    }
    catch (Exception e) {
      Log.getLogWriter()
          .info("Exception occured while updating SharedCounter ");
      exceptionMsg.append(e.getMessage());
      exceptionOccured = true;
    }
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

}
