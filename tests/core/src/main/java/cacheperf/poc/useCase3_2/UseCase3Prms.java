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

package cacheperf.poc.useCase3_2;

import hydra.*;
import java.util.*;

/**
 * A class used to store keys for test configuration settings.
 */
public class UseCase3Prms extends BasePrms {

  public static enum CacheOp {
    get, put, getAll, putAll;
    public static CacheOp toCacheOp(String cacheOp) {
      if (cacheOp.equalsIgnoreCase("get"))    return get;
      if (cacheOp.equalsIgnoreCase("put"))    return put;
      if (cacheOp.equalsIgnoreCase("getAll")) return getAll;
      if (cacheOp.equalsIgnoreCase("putAll")) return putAll;
      throw new HydraConfigException("Unknown cacheOp: " + cacheOp);
    }
  }

  public static enum ClientName {
    data, server;
    public static ClientName toClientName(String clientName) {
      if (clientName.equalsIgnoreCase("data"))   return data;
      if (clientName.equalsIgnoreCase("server")) return server;
      throw new HydraConfigException("Unknown clientName: " + clientName);
    }
  }

  public static enum RegionName {
    Par, ParPersist, Rep, RepPersist;
    public static RegionName toRegionName(String regionName) {
      if (regionName.equalsIgnoreCase("Par"))        return Par;
      if (regionName.equalsIgnoreCase("ParPersist")) return ParPersist;
      if (regionName.equalsIgnoreCase("Rep"))        return Rep;
      if (regionName.equalsIgnoreCase("RepPersist")) return RepPersist;
      throw new HydraConfigException("Unknown region: " + regionName);
    }
  }

  private static final String CACHE_PERF_NONE = "none";

  //----------------------------------------------------------------------------
  //  Key-Value management
  //----------------------------------------------------------------------------

  /**
   * (String)
   * Type of keys to create, as a fully qualified classname.  The class can
   * be any class on the classpath.  Defaults to {@link java.lang.String}.
   * Not for use with ONEOF or RANGE or as a task parameter.
   *
   * @see objects.ObjectHelper
   */
  public static Long keyType;
  public static String getKeyType() {
    Long key = keyType;
    return tab().stringAt(key, "java.lang.String");
  }

  private static final String DEFAULT_KEY_ALLOCATION = "sameKeysWrap";

  /**
   *  (String)
   *  The method to use when allocating keys to threads for cache operations.
   *  Options are "sameKey", "sameKeys", "sameKeysWrap", "sameKeysRandomWrap",
   *  "ownKey", "ownKeys", "ownKeysWrap", "ownKeysRandomWrap", "ownKeysChunked".
   *  Defaults to {@link #DEFAULT_KEY_ALLOCATION}.
   */
  public static Long keyAllocation;
  public static final int SAME_KEY              = 0;
  public static final int SAME_KEYS             = 1;
  public static final int SAME_KEYS_WRAP        = 2;
  public static final int SAME_KEYS_RANDOM_WRAP = 3;
  public static final int OWN_KEY               = 4;
  public static final int OWN_KEYS              = 5;
  public static final int OWN_KEYS_WRAP         = 6;
  public static final int OWN_KEYS_RANDOM_WRAP  = 7;
  public static final int OWN_KEYS_CHUNKED      = 8;
  public static int getKeyAllocation() {
    Long key = keyAllocation;
    String val = tasktab().stringAt( key, tab().stringAt( key, DEFAULT_KEY_ALLOCATION ) );
    if ( val.equalsIgnoreCase( "sameKey" ) ) {
      return SAME_KEY;
    } else if ( val.equalsIgnoreCase( "sameKeys" ) ) {
      return SAME_KEYS;
    } else if ( val.equalsIgnoreCase( "sameKeysWrap" ) ) {
      return SAME_KEYS_WRAP;
    } else if ( val.equalsIgnoreCase( "sameKeysRandomWrap" ) ) {
      return SAME_KEYS_RANDOM_WRAP;
    } else if ( val.equalsIgnoreCase( "ownKey" ) ) {
      return OWN_KEY;
    } else if ( val.equalsIgnoreCase( "ownKeys" ) ) {
      return OWN_KEYS;
    } else if ( val.equalsIgnoreCase( "ownKeysWrap" ) ) {
      return OWN_KEYS_WRAP;
    } else if ( val.equalsIgnoreCase( "ownKeysRandomWrap" ) ) {
      return OWN_KEYS_RANDOM_WRAP;
    } else if ( val.equalsIgnoreCase( "ownKeysChunked" ) ) {
      return OWN_KEYS_CHUNKED;
    } else {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
  }

  /**
   * (int)
   * The size of each "chunk" in a chunked {@link keyAllocation}.
   * Defaults to 1.  Not for use with oneof or range.
   */
  public static Long keyAllocationChunkSize;
  public static int getKeyAllocationChunkSize() {
    Long key = keyAllocationChunkSize;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /**
   *  (String)
   *  Type of objects to create, as a fully qualified classname.  The class can
   *  be any supported by {@link objects.ObjectHelper}.  Defaults to
   *  {@link objects.SizedString} with the default size.
   *
   *  @see objects.ObjectHelper
   */
  public static Long objectType;
  public static String getObjectType() {
    Long key = objectType;
    return tasktab().stringAt( key, tab().stringAt( key, "objects.SizedString" ) );
  }

  /**
   * (boolean)
   * Whether to allow gets to return null values without causing test failure.
   * Defaults to false.
   */
  public static Long allowNulls;
  public static boolean allowNulls() {
    Long key = allowNulls;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  //----------------------------------------------------------------------------
  //  Terminators
  //----------------------------------------------------------------------------

  private static final String DEFAULT_CLASS = "cacheperf.poc.useCase3_2.Terminators";

  /**
   *  (int)
   *  The number of seconds in each batch for the current task
   *  Defaults to -1 (not usable).
   */
  public static Long batchSeconds;
  public static int getBatchSeconds() {
    Long key = batchSeconds;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val;
  }

  /**
   *  (String)
   *  The class name to invoke at the termination of each batch.
   *  Defaults to {@link #DEFAULT_CLASS}.
   *  See also {@link #batchTerminatorMethod}.
   */
  public static Long batchTerminatorClass;
  public static String getBatchTerminatorClass() {
    Long key = batchTerminatorClass;
    return tasktab().stringAt( key, tab().stringAt( key, DEFAULT_CLASS ) );
  }

  /**
   *  (String)
   *  The method to invoke to check for batch termination.  The method
   *  return type is assumed to be void.  Defaults to "terminateOnBatchSecodns".
   *  See also {@link #batchTerminatorClass}.
   */
  public static Long batchTerminatorMethod;
  public static String getBatchTerminatorMethod() {
    return getTerminatorMethod( batchTerminatorMethod, "terminateOnBatchSecodns" );
  }

  /**
   *  (String)
   *  The class name to invoke to check for task termination.
   *  Defaults to {@link #DEFAULT_CLASS}.
   *  See also {@link #taskTerminatorMethod}.
   */
  public static Long taskTerminatorClass;
  public static String getTaskTerminatorClass() {
    Long key = taskTerminatorClass;
    return tasktab().stringAt( key, tab().stringAt( key, DEFAULT_CLASS ) );
  }

  /**
   *  (String)
   *  The method to invoke to check for task termination.  The method
   *  return type must be boolean.  Defaults to "terminateOnTotalTaskTimeSec".
   *  See also {@link #taskTerminatorClass}.
   */
  public static Long taskTerminatorMethod;
  public static String getTaskTerminatorMethod() {
    return getTerminatorMethod( taskTerminatorMethod, "terminateOnTotalTaskTimeSec" );
  }

  private static String getTerminatorMethod( Long key, String dfault ) {
    String val = tasktab().stringAt( key, tab().stringAt( key, dfault ) );
    return val == null || val.equalsIgnoreCase( CACHE_PERF_NONE ) ? null : val;
  }

  //----------------------------------------------------------------------------
  //  Bounce task params
  //----------------------------------------------------------------------------

  /**
   * (String)
   * The logical name of the client to bounce when executing {@link
   * UseCase3Client#bounceTask}.
   */
  public static Long clientNameToBounce;
  public static ClientName getClientNameToBounce() {
    Long key = clientNameToBounce;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = "Missing " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return ClientName.toClientName(val);
  }

  /**
   * (boolean)
   * Whether to use a mean kill when executing {@link UseCase3Client#bounceTask}.
   * Defaults to false.
   */
  public static Long useMeanKill;
  public static boolean useMeanKill() {
    Long key = useMeanKill;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (int)
   * The number of seconds to wait before stopping a VM that executes a
   * {@link UseCase3Client#bounceTask}.  Defaults to 0.
   */
  public static Long stopWaitSec;
  public static int getStopWaitSec() {
    Long key = stopWaitSec;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0 && val != -1) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The number of seconds to wait before restarting a VM that executes a
   * {@link UseCase3Client#bounceTask}.  Defaults to 0.
   */
  public static Long restartWaitSec;
  public static int getRestartWaitSec() {
    Long key = restartWaitSec;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0 && val != -1) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Time to sleep after each cache operation, in milliseconds.  Defaults to
   * 1000.
   */
  public static Long sleepAfterOpMs;
  public static int getSleepAfterOpMs() {
    Long key = sleepAfterOpMs;
    return tasktab().intAt(key, tab().intAt(key, 1000));
  }

  //----------------------------------------------------------------------------
  //  Interest
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The total number of keys to register interest in.  Default is 1.
   * Not for use with oneof or range.
   */
  public static Long interestTotalKeys;
  public static int getInterestTotalKeys() {
    Long key = interestTotalKeys;
    int val = tasktab().intAt(key, tab().intAt(key, 1));
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + "=" + val
               + "  must be greater than 0";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The number of keys to register interest in at one time.  Default is 10.
   * Not for use with oneof or range.
   * <p>
   * The size should be sufficient to justify registering with a list.
   */
  public static Long interestBatchSize;
  public static int getInterestBatchSize() {
    Long key = interestBatchSize;
    int val = tasktab().intAt(key, tab().intAt(key, 10));
    if (val <= 1 ) {
      String s = BasePrms.nameForKey(key) + "=" + val
               + "  must be greater than 1";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Regions
  //----------------------------------------------------------------------------

  /**
   * (List of String)
   * List of region configs to use when creating regions.
   */
  public static Long regionConfigs;
  public static List getRegionConfigs() {
    Long key = regionConfigs;
    return tasktab().vecAt(key, tab().vecAt(key, null));
  }

  /**
   * (Comma-separated list of String/int pairs)
   * Maps each regionName to its maxKey, which range from 0 to maxKey - 1.
   * Not intended for use with oneof or range.
   */
  public static Long regionSpec;
  public static Map<RegionName,Integer> getRegionSpec() {
    Long key = regionSpec;
    Vector vv = tasktab().vecAt(key, tab().vecAt(key, null));
    if (vv == null) {
      String s = "Missing " + nameForKey(key) + ": " + vv;
      throw new HydraConfigException(s);
    }
    Map<RegionName, Integer> map = new HashMap();
    for (Iterator i = vv.iterator(); i.hasNext();) {
      Vector v = (Vector)i.next();
      if (v.size() != 2) {
        String s = "Malformed " + nameForKey(key) + ": " + vv;
        throw new HydraConfigException(s);
      }
      RegionName rname = RegionName.toRegionName((String)v.get(0));
      int mkey;
      try {
        mkey = Integer.parseInt((String)v.get(1));
      } catch (NumberFormatException e) {
        String s = "Malformed " + nameForKey(key) + ": " + vv;
        throw new HydraConfigException(s);
      }
      map.put(rname, mkey);
    }
    return map;
  }

  /**
   * (String)
   * The name of a region as specified in the {@link #regionSpec}.
   */
  public static Long regionName;
  public static RegionName getRegionName() {
    Long key = regionName;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = "Missing " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return RegionName.toRegionName(val);
  }

  /**
   * (Comma-separated list of String/double pairs)
   * Maps each cacheOp to its percentage, which ranges from 0 to 100.
   * Percentages must add up to 100.
   */
  public static Long cacheOpSpec;
  public static Map<CacheOp,Double> getCacheOpSpec() {
    Long key = cacheOpSpec;
    Vector vv = tasktab().vecAt(key, tab().vecAt(key, null));
    if (vv == null) {
      String s = "Missing " + nameForKey(key) + ": " + vv;
      throw new HydraConfigException(s);
    }
    double total = 0.0;
    Map<CacheOp, Double> map = new HashMap();
    for (Iterator i = vv.iterator(); i.hasNext();) {
      Vector v = (Vector)i.next();
      if (v.size() != 2) {
        String s = "Malformed " + nameForKey(key) + ": " + vv;
        throw new HydraConfigException(s);
      }
      CacheOp op = CacheOp.toCacheOp((String)v.get(0));
      double weight;
      try {
        weight = Double.parseDouble((String)v.get(1))/100d;
      } catch (NumberFormatException e) {
        String s = "Malformed " + nameForKey(key) + ": " + vv;
        throw new HydraConfigException(s);
      }
      total += weight;
      map.put(op, weight);
    }
    if (total != 1) {
      String s = nameForKey(key) + " percentages do not add up to 100: " + vv;
      throw new HydraConfigException(s);
    }
    return map;
  }

  /**
   * (int)
   * The number of entries in the bulk op map.  Default is 1.
   */
  public static Long bulkOpMapSize;
  public static int getBulkOpMapSize() {
    Long key = bulkOpMapSize;
    int val = tasktab().intAt(key, tab().intAt(key, 1));
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + " must be greater than 0: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(UseCase3Prms.class);
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
