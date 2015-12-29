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

package memscale;

import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.StopSchedulingTaskOnClientOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import objects.ArrayOfBytePrms;
import objects.ObjectHelper;
import util.TestException;
import util.TestHelper;
import cacheperf.CachePerfClient;
import cacheperf.CachePerfPrms;

/**
 * @author lynn
 *
 */
public class MemScaleTest extends CachePerfClient {

  private static final String CREATE_STEP = "Creating";
  private static final String DESTROY_STEP = "Destroying";
  private static HydraThreadLocal threadLocal_currentTaskStep = new HydraThreadLocal();
  private static HydraThreadLocal threadLocal_isLeader = new HydraThreadLocal();

  // used for setting alternate maxKeys/objectSize during a run
  private int currentSize = 0; // the current object size being used when sizes change during a run
  private static List<Integer> alternateMaxKeysList = null;
  private static List<Integer> alternateObjectSizesList = null;

  public static void HydraTask_initialize() {
    threadLocal_currentTaskStep.set(DESTROY_STEP);
    MemScaleBB.getBB().getSharedCounters().setIfLarger(MemScaleBB.currentExecutionCycle, 1);
    Log.getLogWriter().info("Current execution cycle: " +
        MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.currentExecutionCycle));
    initAlternateValues();
  }

  /** Task to do creates, then destroy a percentage of the cache (up to 100% of the cache).
   *  This simulates a Hadoop function of creating lots of data, then destroying it,
   *  creating it, destroying it etc.
   */
  public static void HydraTask_destroyTest() {
    // determine if this thread is the leader; if so this thread is leader for the duration of the test
    Object value = threadLocal_isLeader.get();
    boolean isLeader = false;
    if (value == null) {
      long leaderCounter = MemScaleBB.getBB().getSharedCounters().incrementAndRead(MemScaleBB.leader);
      isLeader = (leaderCounter == 1);
      threadLocal_isLeader.set(isLeader);
    } else {
      isLeader = (Boolean)value;
    }
    Log.getLogWriter().info("isLeader: " + isLeader);

    // execute either the create step or the destroy step
    String currentTaskStep = (String)(threadLocal_currentTaskStep.get());
    if (currentTaskStep.equals(CREATE_STEP)) {
      try {
        Log.getLogWriter().info("Creating data...");
        memScaleCreateData();
        return;
      } catch (StopSchedulingTaskOnClientOrder e) {
        if (isLeader) {
          TestHelper.waitForCounter(MemScaleBB.getBB(), "doneCreating", MemScaleBB.doneCreating, CachePerfClient.numThreads()-1, true, -1, 500);
          Log.getLogWriter().info("Zeroing doneDestroying");
          MemScaleBB.getBB().getSharedCounters().zero(MemScaleBB.doneDestroying);
          CachePerfClient.sleepTask(); // defaults to 0; used with local conf to verify test 
          MemScaleBB.getBB().getSharedCounters().increment(MemScaleBB.doneCreating);
        } else {
          MemScaleBB.getBB().getSharedCounters().increment(MemScaleBB.doneCreating);
          TestHelper.waitForCounter(MemScaleBB.getBB(), "doneCreating", MemScaleBB.doneCreating, CachePerfClient.numThreads(), true, -1, 100);
        }
        CachePerfClient.resetPseudoRandomUniqueKeysTask();
        Log.getLogWriter().info("Changing to destroy step");
        currentTaskStep = DESTROY_STEP;
        threadLocal_currentTaskStep.set(currentTaskStep);
        if (isLeader) {
          long counter = MemScaleBB.getBB().getSharedCounters().incrementAndRead(MemScaleBB.currentExecutionCycle);
          Log.getLogWriter().info("Current execution cycle: " + counter);
        }
      }
    } else if (currentTaskStep.equals(DESTROY_STEP)) {
      try {
        boolean useClear = MemScalePrms.getUseClear();
        if (useClear) {
          Log.getLogWriter().info("Clearing region...");
          CachePerfClient.clearTask();
          throw new StopSchedulingTaskOnClientOrder("Done with clear");
        } else {
          Log.getLogWriter().info("Destroying data...");
          memScaleDestroyData();
        }
        return;
      } catch (StopSchedulingTaskOnClientOrder e) {
        long counter = MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.currentExecutionCycle);
        Log.getLogWriter().info("Determining if it is time to stop, current execution cycle is " + counter);
        if (counter >= MemScalePrms.getNumberExecutionCycles()) {
          throw new StopSchedulingTaskOnClientOrder("Terminating test because " + counter + " test cycles have completed");
        }
        if (isLeader) {
          TestHelper.waitForCounter(MemScaleBB.getBB(), "doneDestroying", MemScaleBB.doneDestroying, CachePerfClient.numThreads()-1, true, -1, 500);
          Log.getLogWriter().info("Zeroing doneCreating");
          MemScaleBB.getBB().getSharedCounters().zero(MemScaleBB.doneCreating);
          CachePerfClient.sleepTask(); // defaults to 0; used with local conf to verify test 
          advanceAlternateValues();
          MemScaleBB.getBB().getSharedCounters().increment(MemScaleBB.doneDestroying);
        } else {
          MemScaleBB.getBB().getSharedCounters().increment(MemScaleBB.doneDestroying);
          TestHelper.waitForCounter(MemScaleBB.getBB(), "doneDestroying", MemScaleBB.doneDestroying, CachePerfClient.numThreads(), true, -1, 100);
        }
        CachePerfClient.resetPseudoRandomUniqueKeysTask();
        if (alternateMaxKeysList != null) { // using alternate maxKeys/object sizes
          MemScaleTest memScale = new MemScaleTest();
          memScale.setNumPseudoRandomKeys(-1);
        }
        Log.getLogWriter().info("Changing to create step");
        currentTaskStep = CREATE_STEP;
        threadLocal_currentTaskStep.set(currentTaskStep);
      }
    } else {
      throw new TestException("Unknown task step " + currentTaskStep);
    } 
  }

  /** Equivalent of CachePerfClient.createDataTask()
   * 
   */
  private static void memScaleCreateData() {
    MemScaleTest memScale = new MemScaleTest();
    memScale.initialize(CREATES);
    setAlternateValues(memScale); 
    memScale.createData();
  }

  /** Equivalent of CachePerfClient.destroyDataTask()
   * 
   */
  private static void memScaleDestroyData() {
    MemScaleTest memScale = new MemScaleTest();
    memScale.initialize( DESTROYS );
    setAlternateValues(memScale);
    memScale.destroyData();
  }

  /** Set values used for alternate keys/object sizes during the run
   * 
   */
  private static synchronized void initAlternateValues() throws TestException {
    Vector alternateMaxKeysParam = MemScalePrms.getAlternateMaxKeys();
    Vector alternateObjectSizesParam = MemScalePrms.getAlternateObjectSizes();
    if ((alternateMaxKeysParam == null) || (alternateObjectSizesParam == null)) { // at least one of them is null
      if ((alternateMaxKeysParam != null) || (alternateObjectSizesParam != null)) { // at least one of them is not null
        throw new TestException(MemScalePrms.class.getName() + ".alternateObjectSizes and " + MemScalePrms.class.getName() + "-alternateMaxKeys must either both be set or neither set");
      }
      return; // both are null
    }
    // both are set
    if (alternateMaxKeysParam.size() != alternateObjectSizesParam.size()) {
      throw new TestException(MemScalePrms.class.getName() + ".alternateMaxKeys is size " + alternateMaxKeysParam.size() + ", but must be the same size as " +
          MemScalePrms.class.getName() + "-alternateObjectSizes of size " + alternateObjectSizesParam.size());
    }
    // both are the same size
    if (alternateMaxKeysParam.size() == 0) {
      throw new TestException(MemScalePrms.class.getName() + ".alternateMaxKeys is size 0");
    }
    // now convert the string contents to integers
    alternateMaxKeysList = new ArrayList<Integer>();
    alternateObjectSizesList = new ArrayList<Integer>();
    for (int i = 0; i < alternateMaxKeysParam.size(); i++) {
      alternateMaxKeysList.add(Integer.valueOf((String)(alternateMaxKeysParam.get(i))));
      alternateObjectSizesList.add(Integer.valueOf((String)(alternateObjectSizesParam.get(i))));
    }
    Log.getLogWriter().info("alternateMaxKeys is " + alternateMaxKeysList + ", alternateSizes is " + alternateObjectSizesList);
    MemScaleBB.getBB().getSharedCounters().setIfLarger(MemScaleBB.index, alternateMaxKeysList.size()); // init so that the first destroy step uses the original values (not the alternates)
  }

  /** Set alternate values for maxKeys and objectSize if this test is running with alternate values
   * 
   * @param memScale The test instance to set the alternate values in.
   */
  private static void setAlternateValues(MemScaleTest memScale) {
    int index = (int) MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.index);
    if (alternateMaxKeysList != null) { // using alternate values
      if (index < alternateMaxKeysList.size()) { // index is in range
        memScale.maxKeys = (Integer) alternateMaxKeysList.get(index);
        memScale.currentSize = (Integer) alternateObjectSizesList.get(index);
        // in the cacheperf framework, maxKeys is scaled up by multiplying by the number of members, so do that here to keep
        // in the spirit of the framework
        int numMembers = MemScalePrms.getNumMembers();
        memScale.maxKeys *= numMembers;
      } else { // use the original settings for the test (already scaled by the framework to multiply by number of members)
        memScale.maxKeys = CachePerfPrms.getMaxKeys();
        memScale.currentSize = ArrayOfBytePrms.getSize();
      }
      Log.getLogWriter().info("Using maxKeys " + memScale.maxKeys + ", objectSize " + memScale.currentSize);
    }
  }

  private static void advanceAlternateValues() {
    if (alternateMaxKeysList != null) { // using alternate values in this run
      int index = (int) MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.index);
      if (index >= alternateMaxKeysList.size()) { // we used default values last time, reset to 0
        MemScaleBB.getBB().getSharedCounters().zero(MemScaleBB.index);
      } else {
        MemScaleBB.getBB().getSharedCounters().increment(MemScaleBB.index);
      }
      Log.getLogWriter().info("Advanced index used to choose maxKeys/objectSizes to " + MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.index));
    }
  }

  //=====================================================================================================
  // overridden methods

  @Override
  protected void create( int i ) {
    if (alternateMaxKeysList != null) { // test is running with maxKeys/objectSize variations
      Object key = ObjectHelper.createName( this.keyType, i );
      Object val = new byte[currentSize];
      long start = this.statistics.startCreate();
      this.cache.create( key, val );
      this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
    } else {
      super.create(i);
    }
  }

}
