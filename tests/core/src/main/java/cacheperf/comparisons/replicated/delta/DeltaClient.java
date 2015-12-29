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

package cacheperf.comparisons.replicated.delta;

import cacheperf.CachePerfClient;
import cacheperf.CachePerfException;
import cacheperf.CachePerfPrms;
import com.gemstone.gemfire.Delta;
import hydra.HydraConfigException;
import hydra.HydraRuntimeException;
import java.util.List;
import objects.ConfigurableObject;
import objects.ObjectHelper;
import objects.UpdatableObject;
import perffmwk.PerfStatException;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;

/**
 * Client used to measure delta performance.
 */
public class DeltaClient extends CachePerfClient {

  private static final boolean enforceDeltaObjectsOnly =
                               DeltaPrms.enforceDeltaObjectsOnly();

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * TASK to update existing objects.  The objects must implement {@link
   * objects.ConfigurableObject}, {objects.UpdatableObject}, and {@link
   * com.gemstone.gemfire.Delta}.
   * <p>
   * Each object to update is obtained either by getting the object from the
   * cache or by instantiating a bare-bones copy, depending on the value of
   * {@link DeltaPrms#getBeforeUpdate}.  The update method is invoked on the
   * object and it is put back into the cache.
   * <p>
   * @throws HydraConfigException
   *         if the object does not implement the required interfaces
   * @throws HydraRuntimeException
   *         if the object has not been created yet
   */
  public static void updateDeltaDataTask() {
    DeltaClient c = new DeltaClient();
    c.initialize(UPDATES);
    c.updateDeltaData();
  }
  private void updateDeltaData() {
    boolean getBeforeUpdate = DeltaPrms.getBeforeUpdate();
    int numUpdates = CachePerfPrms.getNumUpdates();
    if (this.useTransactions) {
      this.begin();
    }
    boolean batchDone = false;
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      updateDelta(key, getBeforeUpdate, numUpdates);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
  private void updateDelta(int i, boolean getBeforeUpdate, int numUpdates) {
    Object key = ObjectHelper.createName(this.keyType, i);
    Object val = null;
    long start = this.statistics.startUpdate();
    if (getBeforeUpdate) {
      val = this.cache.get(key);
      if (val == null) {
        String s = "Key has not been created: " + i;
        throw new HydraRuntimeException(s);
      }
    } else {
      String objectType = CachePerfPrms.getObjectType();
      val = createUninitializedObject(objectType, i);
    }
    if (enforceDeltaObjectsOnly && !(val instanceof Delta)) {
      String s = val.getClass().getName() + " does not implement Delta";
      throw new HydraRuntimeException(s);
    }
    for (int n = 0; n < numUpdates; n++) {
      ObjectHelper.update(val);
      this.cache.put(key, val);
    }
    this.statistics.endUpdate(start, numUpdates, this.isMainWorkload,
                                                 this.histogram);
  }

  /**
   * Generates an {@link objects.ConfigurableObject} of the specified type but
   * does not invoke {@link objects.ConfigurableObject#init} on it.  Note that,
   * in addition to being uninitialized, the returned object is not indexed.
   *
   * @throws HydraConfigException
   *         The class is not a supported type or could not be found.
   */
  private static Object createUninitializedObject(String classname, int index) {
    try {
      Class cls = Class.forName(classname);
      return (ConfigurableObject)cls.newInstance();
    }
    catch (ClassCastException e) {
      String s = classname + " is not a ConfigurableObject";
      throw new HydraConfigException(s, e);
    }
    catch (ClassNotFoundException e) {
      String s = "Unable to find class for type " + classname;
      throw new HydraConfigException(s, e);
    }
    catch (IllegalAccessException e) {
      String s = "Unable to instantiate object of type " + classname;
      throw new HydraConfigException(s, e);
    }
    catch (InstantiationException e) {
      String s = "Unable to instantiate object of type " + classname;
      throw new HydraConfigException(s, e);
    }
  }

  /**
   * ENDTASK to validate that delta propagation occurred.
   * <p>
   * @throws HydraRuntimeException
   *         if aggregated DistributionStats.processedDeltaMessages is 0.
   */
  public static void validateDeltaPropagationTask() {
    double deltas = readStat("CachePerfStats", "deltaUpdates");
    if (deltas < 1) {
      String s = "No delta propagation occurred -- "
               + "CachePerfStats.deltaUpdates=" + deltas;
      throw new HydraRuntimeException(s);
    }
  }
  protected static double readStat(String statType, String statName) {
    String spec = "* " // search all archives
      + statType + " " + "* " // match all instances
      + statName + " "
      + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
      + StatSpecTokens.COMBINE_TYPE + "="
                       + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
      + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    final int maxTries = 500; // workaround for bug 30288
    String errStr = null;
    for (int tryCount = 1; tryCount <= maxTries; tryCount++) {
      List aList = null;
      try {
         aList = PerfStatMgr.getInstance().readStatistics(spec, true);
      } catch (ArrayIndexOutOfBoundsException e) {
         errStr = e.toString();
         continue;
      }
      if (aList == null) {
        throw new PerfStatException("Statistics not found: " + spec);
      } else if (aList.size() == 1) {
        StringBuffer aStr = new StringBuffer();
        PerfStatValue stat = (PerfStatValue)aList.get(0);
        double currValue = stat.getMax();
        aStr.append(statName + ": " + stat + "\n");
        return currValue;
      } else {
        throw new PerfStatException("Too many statistics: " + aList.size());
      }
    }
    throw new CachePerfException("Could not get stats in " + maxTries
                                + " attempts; " + errStr);
  }
}
