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

package gfxdperf;

import gfxdperf.terminators.TerminatorFactory.TerminatorName;
import hydra.BasePrms;
import hydra.HydraConfigException;
import hydra.TestTask;

/**
 * A class used to store keys for test configuration settings.
 */
public class PerfPrms extends BasePrms {
  
  static {
    setValues(PerfPrms.class);
  }

  private static final String DEFAULT_KEY_ALLOCATION = "SameKeysWrap";

  public static enum KeyAllocation {
    SameKey, SameKeys, SameKeysWrap, SameKeysRandomWrap,
    OwnKey, OwnKeys, OwnKeysWrap, OwnKeysRandomWrap,
    OwnKeysChunked, OwnKeysChunkedRandomWrap,
    Zipfian, ScrambledZipfian
    ;
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether this task is the main workload.  Use as a task attribute on the
   * task that is the main workload for the test.  Only one task can be the
   * main workload.  Not honored by listeners or update event statistics.
   * Defaults to false.
   * <p>
   * Transparently shadows the trim interval and statistics for the task as
   * "ops", without adding another call to System.currentTimeMillis().
   */
  public static Long isMainWorkload;

  public static boolean isMainWorkload() {
    Long key = isMainWorkload;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Name of a key allocation scheme found in {@link #KeyAllocation} used to
   * allocate keys to threads in {@link PerfClient#getNextKey}. Defaults to
   * {@link #DEFAULT_KEY_ALLOCATION}.
   */
  public static Long keyAllocation;
  public static KeyAllocation getKeyAllocation() {
    Long key = keyAllocation;
    String val = tasktab().stringAt(key, tab().stringAt(key,
                                    DEFAULT_KEY_ALLOCATION));
    for (KeyAllocation k : KeyAllocation.values()) {
      if (val.equalsIgnoreCase(k.toString())) {
        return k; 
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s); 
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * The size of each "chunk" in a chunked {@link #keyAllocation}.
   * Defaults to 1. Not for use with oneof or range.
   */
  public static Long keyAllocationChunkSize;

  public static int getKeyAllocationChunkSize() {
    Long key = keyAllocationChunkSize;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /**
   * Throws an exception if {@link #maxKeys} does not evenly divide {@link
   * #keyAllocationChunkSize} when using the key allocation scheme
   * "OwnKeysChunkedRandomWrap".
   */
  public static void checkOwnKeysChunkedRandomWrap(int max, int chunk) {
    if (max % chunk != 0) {
      String s = BasePrms.nameForKey(keyAllocationChunkSize) + "=" + chunk
               + " does not evenly divide "
               + BasePrms.nameForKey(maxKeys) + "=" + max;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * The maximum number of keys used in the test, where <code>maxKeys-1</code>
   * is the maximum key. Defaults to -1 (unused).
   */
  public static Long maxKeys;

  public static int getMaxKeys() {
    Long key = maxKeys;
    return tasktab().intAt(key, tab().intAt(key, -1));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * The number of WAN sites in the test. Returns 0 if not set, which defaults
   * to the number computed by {@link PerfClient#numWanSites}, which assumes a
   * specific format for client names.
   */
  public static Long numWanSites;

  public static int getNumWanSites() {
    Long key = numWanSites;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to reset the current key after the task has stopped being
   * scheduled. Defaults to true.
   */
  public static Long resetKeysAfterTaskEnds;

  public static boolean resetKeysAfterTaskEnds(TestTask task) {
    Long key = resetKeysAfterTaskEnds;
    return task.getTaskAttributes().booleanAt(key, tab().booleanAt(key, true));
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to reset the current key before the task has started being
   * scheduled. Defaults to true.
   */
  public static Long resetKeysBeforeTaskStarts;

  public static boolean resetKeysBeforeTaskStarts(TestTask task) {
    Long key = resetKeysBeforeTaskStarts;
    return task.getTaskAttributes().booleanAt(key, tab().booleanAt(key, true));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * The number of milliseconds to sleep after syncing. Defaults to 0. Useful
   * for making synchronization points at the end of warmup obvious in VSD.
   */
  public static Long syncSleepMs;

  public static int getSyncSleepMs() {
    return tasktab().intAt(syncSleepMs, tab().intAt(syncSleepMs, 0));
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Name of a terminator found in {@link #TerminatorName} to use in a task.
   */
  public static Long terminatorName;

  public static TerminatorName getTerminatorName() {
    Long key = terminatorName;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      return null;
    }
    for (TerminatorName t : TerminatorName.values()) {
      if (val.equalsIgnoreCase(t.toString())) {
        return t;
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s);
  }
}
