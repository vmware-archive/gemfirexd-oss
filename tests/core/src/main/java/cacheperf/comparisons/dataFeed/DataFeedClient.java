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

package cacheperf.comparisons.dataFeed;

import cacheperf.*;
import com.gemstone.gemfire.internal.NanoTimer;
import hydra.*;
import java.util.List;
import objects.*;
import perffmwk.*;

/**
 * Specially tuned client for data feed testing.
 */

public class DataFeedClient extends CachePerfClient {

  boolean useFixedKeys;
  boolean useFixedVal;
  Object[] fixedKeys;
  Object fixedVal;
  public NanoTimer timer;

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * TASK to put objects.
   */
  public static void feedDataTask() {
    DataFeedClient c = new DataFeedClient();
    c.initialize(PUTS);
    c.feedData();
  }
  private void feedData() {
    boolean batchDone = false;
    int throttledOpsPerSec = DataFeedPrms.getThrottledOpsPerSec();
    if (throttledOpsPerSec > 0) {
      do {
        this.timer.reset();
        for (int i = 0; i < throttledOpsPerSec; i++) {
          batchDone = feed();
        }
        long remaining = 1000000000 - this.timer.reset();
        if (remaining > 0) {
          MasterController.sleepForMs((int)(remaining/1000000d));
        }
      } while (!batchDone);
    } else {
      do {
        batchDone = feed();
      } while (!batchDone);
    }
  }
  private boolean feed() {
    int key = getNextKey();
    executeTaskTerminator();
    executeWarmupTerminator();
    feed(key);
    ++this.batchCount;
    ++this.count;
    ++this.keyCount;
    return executeBatchTerminator();
  }
  private void feed(int i) {
    long start = this.statistics.startPut();
    if (this.useFixedKeys) {
      if (this.useFixedVal) {
        ObjectHelper.resetTimestamp(this.fixedVal);
        this.cache.put(this.fixedKeys[i], this.fixedVal);
      } else {
        String objectType = CachePerfPrms.getObjectType();
        Object val = ObjectHelper.createObject(objectType, i);
        this.cache.put(this.fixedKeys[i], val);
      }
    } else {
      Object key = ObjectHelper.createName(this.keyType, i);
      if (this.useFixedVal) {
        ObjectHelper.resetTimestamp(this.fixedVal);
        this.cache.put(key, this.fixedVal);
      } else {
        String objectType = CachePerfPrms.getObjectType();
        Object val = ObjectHelper.createObject(objectType, i);
        this.cache.put(key, val);
      }
    }
    this.statistics.endPut(start, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to validate that no messages were dropped by bridge servers.
   */
  public static void validateMessagesFailedQueuedTask() {
    DataFeedClient c = new DataFeedClient();
    c.validateMessagesFailedQueued();
  }
  private void validateMessagesFailedQueued() {
    String type = "CacheClientProxyStatistics";
    String stat = "messagesFailedQueued";
    long failures = getStatTotalCount(type, stat);
    String s = type + "." + stat + " = " + failures;
    if (failures == 0) {
      log().info(s);
    } else {
      throw new HydraRuntimeException(s);
    }
  }

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
      double failures = 0;
      for (int i = 0; i < psvs.size(); i++) {
        PerfStatValue psv = (PerfStatValue)psvs.get(i);
        failures += psv.getMax();
      }
      return (long)failures;
    }
  }

  //----------------------------------------------------------------------------
  //  Initialization
  //----------------------------------------------------------------------------

  /**
   * Initialize the hydra thread locals.
   */
  protected void initialize(int trimInterval) {
    super.initialize(trimInterval);

    this.useFixedKeys = useFixedKeys();
    if (this.useFixedKeys) {
      this.fixedKeys = getFixedKeys();
    }
    this.useFixedVal = useFixedVal();
    if (this.useFixedVal) {
      this.fixedVal = getFixedVal();
    }
    this.timer = getTimer();
  }

  /**
   * Gets whether to use fixed keys, lazily setting it and stashing it in a
   * HydraThreadLocal.
   */
  private boolean useFixedKeys() {
    Boolean b = (Boolean)localusefixedkeys.get();
    if (b == null) {
      b = Boolean.valueOf(DataFeedPrms.useFixedKeys());
      localusefixedkeys.set(b);
    }
    return b.booleanValue();
  }

  /**
   * Gets whether to use a fixed value, lazily setting it and stashing it in a
   * HydraThreadLocal.
   */
  private boolean useFixedVal() {
    Boolean b = (Boolean)localusefixedval.get();
    if (b == null) {
      b = Boolean.valueOf(DataFeedPrms.useFixedVal());
      localusefixedval.set(b);
    }
    return b.booleanValue();
  }

  /**
   * Gets the fixed array of maxKeys keys, lazily setting them and stashing them
   * in a HydraThreadLocal.
   */
  private Object[] getFixedKeys() {
    Object[] k = (Object[])localfixedkeys.get();
    if (k == null) {
      k = new Object[this.maxKeys];
      for (int i = 0; i < this.maxKeys; i++) {
        k[i] = ObjectHelper.createName(this.keyType, i);
      }
      localfixedkeys.set(k);
    }
    return k;
  }

  /**
   * Gets the fixed object value, lazily setting it and stashing it in a
   * HydraThreadLocal.
   */
  private Object getFixedVal() {
    Object v = localfixedval.get();
    if (v == null) {
      String objectType = CachePerfPrms.getObjectType();
      v = ObjectHelper.createObject(objectType, 0);
      localfixedval.set(v);
    }
    return v;
  }

  /**
   * Gets the timer, lazily setting it and stashing it in a HydraThreadLocal.
   */
  private NanoTimer getTimer() {
    NanoTimer t = (NanoTimer)localtimer.get();
    if (t == null) {
      t = new NanoTimer();
      localtimer.set(t);
    }
    return t;
  }

  /** Whether to use fixed keys. */
  private static HydraThreadLocal localusefixedkeys = new HydraThreadLocal();

  /** Whether to use a fixed value. */
  private static HydraThreadLocal localusefixedval = new HydraThreadLocal();

  /** The set of keys used in the test, if fixed. */
  private static HydraThreadLocal localfixedkeys = new HydraThreadLocal();

  /** The fixed value used in the test, if fixed. */
  private static HydraThreadLocal localfixedval = new HydraThreadLocal();

  /** The timer used in the test. */
  private static HydraThreadLocal localtimer = new HydraThreadLocal();
}
