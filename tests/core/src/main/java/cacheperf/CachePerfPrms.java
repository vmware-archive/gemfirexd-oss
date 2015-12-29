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

package cacheperf;

import hydra.*;
import java.util.Iterator;
import java.util.Vector;

/**
 *
 *  A class used to store keys for test configuration settings.
 *
 */
public class CachePerfPrms extends BasePrms {

  private static final String CACHE_PERF_NONE = "none";

  //----------------------------------------------------------------------------
  // Trim spec management
  //----------------------------------------------------------------------------

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

  //----------------------------------------------------------------------------
  //  Time-based workload management
  //
  //  @author lises
  //  @since 5.0
  //----------------------------------------------------------------------------

  /**
   *  (int)
   *  The number of warmup iterations to do for the current task.
   *  Defaults to -1 (not usable).  Not for use with oneof or range.
   */
  public static Long trimSeconds;
  public static int getTrimSeconds() {
    Long key = trimSeconds;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val;
  }

  /**
   *  (int)
   *  The number of timing seconds to do for the current task.
   *  Defaults to -1 (not usable).  Not for use with oneof or range.
   */
  public static Long workSeconds;
  public static int getWorkSeconds() {
    Long key = workSeconds;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val;
  }

  /**
   *  (int)
   *  The number of seconds in each batch for the current task
   *  Defaults to -1 (not usable).  Not for use with oneof or range.
   */
  public static Long batchSeconds;
  public static int getBatchSeconds() {
    Long key = batchSeconds;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val;
  }

  //----------------------------------------------------------------------------
  //  Iteration-based workload management
  //----------------------------------------------------------------------------

  /**
   *  (int)
   *  The number of warmup iterations to do for the current task.  This is
   *  divided across all threads in the thread group(s) for the task.
   *  Defaults to -1 (not usable).  Not for use with oneof or range.
   */
  public static Long trimIterations;
  public static int getTrimIterations() {
    Long key = trimIterations;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val == -1 ? val : scale( val );
  }

  /**
   *  (int)
   *  The number of timing iterations to do for the current task.  This is
   *  divided across all threads in the thread group(s) for the task.
   *  Defaults to -1 (not usable).  Not for use with oneof or range.
   */
  public static Long workIterations;
  public static int getWorkIterations() {
    Long key = workIterations;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val == -1 ? val : scale( val );
  }

  /**
   *  (int)
   *  The number of iterations in each batch for the current task.  This
   *  is divided across all threads in the thread group(s) for the task.
   *  Defaults to -1 (not usable).  Not for use with oneof or range.
   */
  public static Long batchSize;
  public static int getBatchSize() {
    Long key = batchSize;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ) );
    return val == -1 ? val : scale( val );
  }

  /**
   *  (int)
   *  The number of iterations in each transaction, on a per-thread basis.
   *  Defaults to 1.
   *  <p>
   *  The batch terminator for a transactional task must have a large enough
   *  granularity to allow the task to perform at least txSize operations in
   *  each complete batch.  Otherwise it is a configuration error.  Fewer than
   *  txSize operations are allowed in a batch that hits task termination before
   *  txSize operations are performed.
   */
  public static Long txSize;
  public static int getTxSize() {
    Long key = txSize;
    int val = tasktab().intAt( key, tab().intAt( key, 1 ) );
    return val;
  }

  private static int scale( int val ) {
    if ( val > 0 ) {
      int numThreads = CachePerfClient.numThreads();
      if ( numThreads == 0 ) croak( val );
      int n = (int) Math.ceil( val / numThreads );
      int rem = val % numThreads;
      RemoteTestModule mod = RemoteTestModule.getCurrentThread();
      int ttgid = mod.getCurrentTask()
                     .getTaskThreadGroupId( mod.getThreadGroupName(),
                                            mod.getThreadGroupId() );
      return ( ttgid < rem ) ? n+1 : n;
    } else {
      return val;
    }
  }
  private static void croak( int val ) {
    RemoteTestModule mod = RemoteTestModule.getCurrentThread();
    hydra.Log.getLogWriter().info( "HEY ERROR: I am scaling " + val +
      " and got 0 numThreads. " + " I am in thread group " + mod.getThreadGroupName() +
      " and have thread group id " + mod.getThreadGroupId() );
  }

  //----------------------------------------------------------------------------
  //  Other workload management
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The number of operations used in {@link Terminators#terminateOnNumOperations}.
   * Not for use with oneof or range.
   */
  public static Long numOperations;
  public static int getNumOperations() {
    Long key = numOperations;
    return tasktab().intAt( key, tab().intAt( key, -1 ) );
  }

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
   *  "ownKey", "ownKeys", "ownKeysWrap", "ownKeysRandomWrap", "ownKeysChunked",
   *  "disjointAsPossible", "pseudoRandomUnique", "zipfian", "scrambledZipfian"
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
  public static final int OWN_KEYS_CHUNKED_RANDOM_WRAP = 9;
  public static final int DISJOINT_AS_POSSIBLE  = 10;
  public static final int PSEUDO_RANDOM_UNIQUE  = 11;
  public static final int ZIPFIAN               = 12;
  public static final int SCRAMBLED_ZIPFIAN     = 13;
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
    } else if ( val.equalsIgnoreCase( "ownKeysChunkedRandomWrap" ) ) {
      return OWN_KEYS_CHUNKED_RANDOM_WRAP;
    } else if ( val.equalsIgnoreCase( "disjointAsPossible" ) ) {
      return DISJOINT_AS_POSSIBLE;
    } else if ( val.equalsIgnoreCase( "pseudoRandomUnique" ) ) {
      return PSEUDO_RANDOM_UNIQUE;
    } else if ( val.equalsIgnoreCase( "zipfian" ) ) {
      return ZIPFIAN;
    } else if ( val.equalsIgnoreCase( "scrambledZipfian" ) ) {
      return SCRAMBLED_ZIPFIAN;
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
   *  (int)
   *  The maximum number of keys used in the test, where <code>maxKeys-1</code>
   *  is the maximum key.  Required parameter.  Not intended for use
   *  with oneof or range.
   */
  public static Long maxKeys;
  public static int getMaxKeys() {
    Long key = maxKeys;
    int val = tasktab().intAt( key, tab().intAt( key ) );
    if ( val > 0 ) {
      return val;
    } else {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
  }

  /**
   *  (boolean)
   *  Whether to reset the current key and key count after the task has stopped
   *  being scheduled.  Defaults to true.
   */
  public static Long resetKeysAfterTaskEnds;
  public static boolean resetKeysAfterTaskEnds( TestTask task ) {
    Long key = resetKeysAfterTaskEnds;
    return task.getTaskAttributes().booleanAt( key, tab().booleanAt( key, true ) );
  }

  /**
   *  (boolean)
   *  Whether to reset the current key and key count before the task has started
   *  being scheduled.  Defaults to true.
   */
  public static Long resetKeysBeforeTaskStarts;
  public static boolean resetKeysBeforeTaskStarts( TestTask task ) {
    Long key = resetKeysBeforeTaskStarts;
    return task.getTaskAttributes().booleanAt( key, tab().booleanAt( key, true ) );
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

  /**
   * (boolean)
   * Whether to validate objects along the way, skipping over null values.
   * Defaults to false.  Use {@link #allowNulls} to configure handling of nulls.
   */
  public static Long validateObjects;
  public static boolean validateObjects() {
    Long key = validateObjects;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  /**
   *  (boolean)
   *  Whether to invalidate objects after gets.  Defaults to false.
   *  The invalidate is local by default, but can be distributed using
   *  {@link #invalidateLocally}.  Not for use with oneof or range.
   *  Not for use with oneof or range.
   */
  public static Long invalidateAfterGet;
  public static boolean invalidateAfterGet() {
    Long key = invalidateAfterGet;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  /**
   *  (boolean)
   *  Whether, when {@link #invalidateAfterGet} is true, to destroy locally
   *  rather than throughout the distributed system.  Defaults to true.
   *  Not for use with oneof or range.
   */
  public static Long invalidateLocally;
  public static boolean invalidateLocally() {
    Long key = invalidateLocally;
    return tasktab().booleanAt( key, tab().booleanAt( key, true ) );
  }

  /**
   *  (boolean)
   *  Whether to destroy objects after gets.  Defaults to false.
   *  The destroy is local by default, but can be distributed using
   *  {@link #destroyLocally}.  Not for use with oneof or range.
   */
  public static Long destroyAfterGet;
  public static boolean destroyAfterGet() {
    Long key = destroyAfterGet;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  /**
   *  (boolean)
   *  Whether, when {@link #destroyAfterGet} is true, to destroy locally
   *  rather than throughout the distributed system.  Defaults to true.
   *  Not for use with oneof or range.
   */
  public static Long destroyLocally;
  public static boolean destroyLocally() {
    Long key = destroyLocally;
    return tasktab().booleanAt( key, tab().booleanAt( key, true ) );
  }

  /**
   *  (boolean)
   *  Whether to sleep before doing an operation.  Defaults to false.
   *  Not for use with oneof or range.
   */
  public static Long sleepBeforeOp;
  public static boolean sleepBeforeOp() {
    Long key = sleepBeforeOp;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  /**
   *  (int)
   *  The time to sleep in milliseconds before doing an operation.
   *  Defaults to 0 (no sleep).  Suitable for oneof, range, etc.
   */
  public static Long sleepMs;
  public static int getSleepMs() {
    Long key = sleepMs;
    int val = tasktab().intAt( key, tab().intAt( key, 0 ) );
    if ( val < 0 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }
  /**
   *  (int)
   *  The number of operations to do after sleeping for {link #sleepMs}.
   *  Defaults to 1 (single op).  Suitable for oneof, range, etc.
   */
  public static Long sleepOpCount;
  public static int getSleepOpCount() {
    Long key = sleepOpCount;
    int val = tasktab().intAt( key, tab().intAt( key, 1 ) );
    if ( val < 1 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }


  //----------------------------------------------------------------------------
  //  Gateways
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The number of keys to put into a gateway queue.  Default is 1.
   * Not for use with oneof or range.
   */
  public static Long gatewayQueueEntries;
  public static int getGatewayQueueEntries() {
    Long key = gatewayQueueEntries;
    double val = tab().doubleAt(key, 1);
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + " must be greater than 0";
      throw new HydraConfigException(s);
    }
    return (int)Math.round(val);
  }

  //----------------------------------------------------------------------------
  //  Bulk ops
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The number of entries in the bulk op map.  Default is 1.
   */
  public static Long bulkOpMapSize;
  public static int getBulkOpMapSize() {
    Long key = bulkOpMapSize;
    int val = tab().intAt(key, 1);
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + " must be greater than 0";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (boolean)
   * Whether to dummy up each bulk op with regular ops.  This allows the test
   * to have the same key allocation and memory use as when using bulk ops.
   * Defaults to false.
   */
  public static Long dummyBulkOps;
  public static boolean getDummyBulkOps() {
    Long key = dummyBulkOps;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  //----------------------------------------------------------------------------
  //  Interest
  //----------------------------------------------------------------------------

  /**
   * (int)
   * The number of keys to register interest in at one time.  Default is 1.
   * Not for use with oneof or range.
   */
  public static Long interestBatchSize;
  public static int getInterestBatchSize() {
    Long key = interestBatchSize;
    int val = tab().intAt(key, 1);
    if (val <= 0 ) {
      String s = BasePrms.nameForKey(key) + " must be greater than 0";
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (boolean)
   * Whether to log regular expressions when registering interest in them.
   * Defaults to false.  Not for use with oneof or range.
   */
  public static Long logInterestRegex;
  public static boolean logInterestRegex() {
    Long key = logInterestRegex;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether or not to register durable (rather than non-durable) interest.
   * Defaults to false.
   */
  public static Long registerDurableInterest;
  public static boolean registerDurableInterest() {
    Long key = registerDurableInterest;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether or not to keep a durable client connection interest list alive
   * when the durable client disconnects.
   */
  public static Long keepDurableInterest;
  public static boolean getKeepDurableInterest() {
    Long key = keepDurableInterest;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether or not to restart VMs after stopping 
   */
  public static Long restartVMs;
  public static boolean restartVMs() {
    Long key = restartVMs;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  //----------------------------------------------------------------------------
  //  Terminators and frequencies
  //----------------------------------------------------------------------------

  private static final String DEFAULT_CLASS = "cacheperf.Terminators";

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
   *  return type is assumed to be void.  Defaults to "terminateOnBatchSize".
   *  See also {@link #batchTerminatorClass}.
   */
  public static Long batchTerminatorMethod;
  public static String getBatchTerminatorMethod() {
    return getTerminatorMethod( batchTerminatorMethod, "terminateOnBatchSize" );
  }

  /**
   *  (String)
   *  The class name to invoke to check for warmup termination.
   *  Defaults to {@link #DEFAULT_CLASS}.
   *  See also {@link #warmupTerminatorMethod}.
   */
  public static Long warmupTerminatorClass;
  public static String getWarmupTerminatorClass() {
    Long key = warmupTerminatorClass;
    return tasktab().stringAt( key, tab().stringAt( key, DEFAULT_CLASS ) );
  }

  /**
   *  (String)
   *  The method to invoke to check for warmup termination.  The method
   *  return type must be boolean.  Defaults to "terminateOnTrimIterations".
   *  See also {@link #warmupTerminatorClass}.
   */
  public static Long warmupTerminatorMethod;
  public static String getWarmupTerminatorMethod() {
    return getTerminatorMethod( warmupTerminatorMethod, "terminateOnTrimIterations" );
  }

  /**
   *  (String)
   *  The frequency with which to check for warmup termination during a task.
   *  Options are "task", "N iterations", "N seconds", where N is an integer.
   *  Defaults to "1 iterations".
   */
  public static Long warmupTerminatorFrequency;
  public static Frequency getWarmupTerminatorFrequency() {
    HydraVector v = new HydraVector();
    v.add( "1" ); v.add( "iterations" );
    return getFrequency( warmupTerminatorFrequency, v );
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
   *  return type must be boolean.  Defaults to "terminateOnTotalIterations".
   *  See also {@link #taskTerminatorClass}.
   */
  public static Long taskTerminatorMethod;
  public static String getTaskTerminatorMethod() {
    return getTerminatorMethod( taskTerminatorMethod, "terminateOnTotalIterations" );
  }

  private static String getTerminatorMethod( Long key, String dfault ) {
    String val = tasktab().stringAt( key, tab().stringAt( key, dfault ) );
    return val == null || val.equalsIgnoreCase( CACHE_PERF_NONE ) ? null : val;
  }

  /**
   *  (String)
   *  The frequency with which to check for task termination during a task.
   *  Options are "task", "N iterations", "N seconds", where N is an integer.
   *  Defaults to "1 iterations".
   */
  public static Long taskTerminatorFrequency;
  public static Frequency getTaskTerminatorFrequency() {
    HydraVector v = new HydraVector();
    v.add( "1" ); v.add( "iterations" );
    return getFrequency( taskTerminatorFrequency, v );
  }

  public static final int PER_TASK  = 0;
  public static final int PER_ITERATION = 1;
  public static final int PER_SECOND    = 2;
  private static Frequency getFrequency( Long key, HydraVector dfault ) {
    HydraVector val = tasktab().vecAt( key, tab().vecAt( key, dfault ) );
    if ( val.size() == 1  ) {
      String type = (String) val.elementAt(0);
      if ( type.equalsIgnoreCase( "task" ) ) {
        return new Frequency( PER_TASK, 0 );
      } else {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
    } else if ( val.size() == 2 ) {
      int n;
      try {
        n = ( new Integer( (String) val.elementAt( 0 ) ) ).intValue();
      } catch( NumberFormatException e ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val, e );
      }
      if ( n < 0 ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
      String type = ( (String) val.elementAt( 1 ) ).toLowerCase();
      if ( type.equals( "iterations" ) ) {
	return new Frequency( PER_ITERATION, n );
      } else if ( type.equals( "seconds" ) ) {
	return new Frequency( PER_SECOND, n );
      } else {
	throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
    } else {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
  }
  public static class Frequency {
    public int type;
    public int frequency;
    public Frequency( int type, int frequency ) {
      this.type = type;
      this.frequency = frequency;
    }
  }

  //----------------------------------------------------------------------------
  //  General use params
  //----------------------------------------------------------------------------
  /**
   *  (int)
   *  Indicates if this test does a mixture of hits & misses
   *  Currently only supported at TestConfig level for 
   *  C cache performance tests.  Defaults to false(0) so existing
   *  tests do not need to be modified.  Note that it is just simpler
   *  for the C tests to deal with ints rather than transforming
   *  Java booleans.
   */
  public static Long hitsAndMisses;


  /**
   *  (int)
   *  The percentage of operations that should be hits.
   *  Note this is currently only used in the C cache performance tests
   *  and is only supported on a per test basis (not as a task attribute).
   */
  public static Long hitPercentage;

  /**
   *  (int)
   *  The percentage of operations that should be puts, for use with tasks that
   *  mix operations, one of which is put.  Default is 50.
   */
  public static Long putPercentage;
  public static int getPutPercentage() {
    Long key = putPercentage;
    int val = tasktab().intAt(key, tab().intAt(key,50));
    if ( val < 0 || val > 100 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  /**
   *  (int)
   *  The minimum number of LRU evictions required for termination of warmup,
   *  or whatever else test code may choose to use this for.
   */
  public static Long LRUEvictions;
  public static int LRUEvictions() {
    return tasktab().intAt( LRUEvictions, tab().intAt( LRUEvictions, 0 ) );
  }

  /**
   *  (int)
   *  The minimum amount of time to wait for LRU evictions to terminate warmup,
   *  or whatever else test code may choose to use this for.  Defaults to 0
   *  (no minimum).
   *  @see LRUEvictions
   */
  public static Long LRUEvictionsMinWaitSec;
  public static int LRUEvictionsMinWaitSec() {
    return tasktab().intAt(LRUEvictionsMinWaitSec,
                           tab().intAt(LRUEvictionsMinWaitSec, 0));
  }

  /**
   *  (int)
   *  The maximum amount of time to wait for LRU evictions to terminate warmup,
   *  or whatever else test code may choose to use this for.  Defaults to 0
   *  (no timeout).
   *  @see LRUEvictions
   */
  public static Long LRUEvictionsMaxWaitSec;
  public static int LRUEvictionsMaxWaitSec() {
    return tasktab().intAt(LRUEvictionsMaxWaitSec,
                           tab().intAt(LRUEvictionsMaxWaitSec, 0));
  }

  /**
   *  (int)
   *  The minimum number of destroys required for termination of warmup,
   *  or whatever else test code may choose to use this for.
   */
  public static Long numDestroys;
  public static int numDestroys() {
    return tasktab().intAt( numDestroys, tab().intAt( numDestroys, 0 ) );
  }

  /**
   *  (int)
   *  The minimum amount of time to wait for destroys to terminate warmup,
   *  or whatever else test code may choose to use this for.  Defaults to 0
   *  (no minimum).
   *  @see numDestroys
   */
  public static Long numDestroysMinWaitSec;
  public static int numDestroysMinWaitSec() {
    return tasktab().intAt(numDestroysMinWaitSec,
                           tab().intAt(numDestroysMinWaitSec, 0));
  }

  /**
   *  (int)
   *  The maximum amount of time to wait for destroys to terminate warmup,
   *  or whatever else test code may choose to use this for.  Defaults to 0
   *  (no timeout).
   *  @see numDestroys
   */
  public static Long numDestroysMaxWaitSec;
  public static int numDestroysMaxWaitSec() {
    return tasktab().intAt(numDestroysMaxWaitSec,
                           tab().intAt(numDestroysMaxWaitSec, 0));
  }

  /**
   * (int)
   * Number of updates to do on the same object.  Defaults to 1.
   * <p>
   * Used by {@link cacheperf.CachePerfClient#updateDataTask}, for example.
   */
  public static Long numUpdates;
  public static int getNumUpdates() {
    Long key = numUpdates;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  /**
   *  (int)
   *  The percentage (roughly) of operations that will attempt to use a recently
   *  used key, rather than obtaining a key with the current key settings (ownKeys,
   *  etc.).  This parameter is an integer between 0 and 100.
   */
  public static Long recentKeysPercentage;
  public static int recentKeysPercentage() {
    return tasktab().intAt( recentKeysPercentage, tab().intAt( recentKeysPercentage, 0 ) );
  }

  /**
   *  (int)
   *  The maximum number of recent keys to consider. This is used with 
   *  recentKeysPercentage above, and indicates that maximum number of
   *  most recently used keys to consider when choosing a key.
   */
  public static Long maxRecentKeys;
  public static int maxRecentKeys() {
    return tasktab().intAt( maxRecentKeys, tab().intAt( maxRecentKeys, 0 ) );
  }

  /**
   *  (int)
   *  The number of milliseconds to sleep before syncing. This is used to give
   *  clients a chance to rest for a while before setting the trim start, which
   *  seems to have a positive effect on client apps. Defaults to 0 (no sleep).
   */
  public static Long syncSleepMs;
  public static int syncSleepMs() {
    return tasktab().intAt( syncSleepMs, tab().intAt( syncSleepMs, 0 ) );
  }

  /**
   *  (boolean)
   *  Whether to encase operations into a transaction.
   *  Defaults to false.
   */
  public static Long useTransactions;
  public static boolean useTransactions() {
    Long key = useTransactions;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  /**
   *  (boolean)
   *  Whether to allow commitConflictException (true), or to rethrow the 
   *  exception if it occurs (false).  Defaults to false.
   */
  public static Long allowConflicts;
  public static boolean allowConflicts() {
    Long key = allowConflicts;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  /**
   *  (int)
   *  The percentage of transactions that should be committed (vs. rollback).
   */
  public static Long commitPercentage;
  public static int getCommitPercentage() {
    Long key = commitPercentage;
    int val = tasktab().intAt( key, tab().intAt( key, 100 ) );
    if ( val < 0 || val > 100 ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
    return val;
  }

  /**
   * (int)
   * The number of executions to carry out.  Defaults to 0.
   * <p>
   * This parameter is useful when a task needs to do something specific on its
   * last scheduled execution (which cannot be done using maxTimesToRun since
   * there is no way for the task to know it is the final execution).
   * <p)
   * For example, {@link CachePerfClient#bounceTask} uses this parameter to
   * execute a fixed number of times then send a signal to clients executing
   * other tasks so that they all terminate together.
   *
   */
  public static Long maxExecutions;
  public static int getMaxExecutions() {
    Long key = maxExecutions;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The number of seconds to wait before restarting a VM that executes a
   * {@link CachePerfClient#bounceTask}.  Defaults to 0.
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
   * (boolean)
   * Whether to use a mean kill when executing {@link
   * CachePerfClient#bounceTask}.  Defaults to false.
   */
  public static Long useMeanKill;
  public static boolean useMeanKill() {
    Long key = useMeanKill;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (String)
   * Client name of VM to bounce when executing {@link CachePerfClient
   * #bounceTask}.  Defaults to null.
   */
  public static Long clientNameToBounce;
  public static String getClientNameToBounce() {
    Long key = clientNameToBounce;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * (boolean)
   * Whether to wait for partitioned region redundancy recovery when
   * executing {@link CachePerfClient#bounceTask}.  Defaults to false.
   * <p>
   * NOTE: Requires ResourceObservers to be installed on datahosts.
   */
  public static Long waitForRecovery;
  public static boolean waitForRecovery() {
    Long key = waitForRecovery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to wait for the trim signal before proceeding.  Defaults to false.
   */
  public static Long waitForTrimSignal;
  public static boolean waitForTrimSignal() {
    Long key = waitForTrimSignal;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   *  (boolean)
   *  Whether to use shutDownAllMembers to stop vms (for use with disk recovery)
   *  Defaults to false.
   */
  public static Long useShutDownAllMembers;
  public static boolean useShutDownAllMembers() {
    Long key = useShutDownAllMembers;
    return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
  }

  //----------------------------------------------------------------------------
  //  Queries
  //----------------------------------------------------------------------------

  /**
   * (String)
   * The query string.  Handles +=.  Defaults to null.
   */
  public static Long query;
  public static String getQuery() {
    Long key = query;
    return getPlusEqualsString(key);
  }

  /**
   * (String)
   * The query index string.  Handles +=.  Defaults to null.
   */
  public static Long queryIndex;
  public static String getQueryIndex() {
    Long key = queryIndex;
    return getPlusEqualsString(key);
  }

  /**
   * (String)
   * The query from caluse.  Handles +=.  Defaults to null.
   */
  public static Long queryFromClause;
  public static String getQueryFromClause() {
    Long key = queryFromClause;
    return getPlusEqualsString(key);
  }

  private static String getPlusEqualsString(Long key) {
    Vector v = tab().vecAt(key, null);
    if (v == null) {
      return null;
    } else {
      String s = "";
      for (Iterator i = v.iterator(); i.hasNext();) {
        s += (String)i.next();
      }
      return s;
    }
  }

  /**
   * (long)
   * The minimum random value for a query range.  Defaults to 0.
   */
  public static Long queryRangeMin;
  public static long getQueryRangeMin() {
    Long key = queryRangeMin;
    long val = tasktab().longAt(key, tab().longAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (long)
   * The maximum random value for a query range.  Defaults to 0.
   */
  public static Long queryRangeMax;
  public static long getQueryRangeMax() {
    Long key = queryRangeMax;
    long val = tasktab().longAt(key, tab().longAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (long)
   * The size of a query range.  Defaults to 0.
   */
  public static Long queryRangeSize;
  public static long getQueryRangeSize() {
    Long key = queryRangeSize;
    long val = tasktab().longAt(key, tab().longAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * The number of WAN sites in the test. Returns 0 if not set, which defaults
   * to the number computed by {@link CachePerfClient#numWanSites}. That method
   * which assumes a certain format for client names.
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

  /**
   * (int)
   * A percentage of maxKeys. Currently this is used for pseudo-random unique
   * keys, but it can be used for other things as well in the future.
   */
  public static Long keyPercentage;
  public static int getKeyPercentage() {
    Long key = keyPercentage;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Optimizations
  //----------------------------------------------------------------------------

  /**
   * (int)
   * Whether, and how much, to optimize loops that support optimization.
   * Default to 1, which means no optimization.  A value of N > 1 instructs
   * the task to optimize by repeating the primary operation N times in an
   * way that skips object creation and levels of indirection, and only
   * incrementing the stat and its timer once for each batch of N operations.
   */
  public static Long optimizationCount;
  public static int getOptimizationCount() {
    Long key = optimizationCount;
    int val = tasktab().intAt(key, tab().intAt(key, 1));
    if (val <= 0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }
  
  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues( CachePerfPrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
