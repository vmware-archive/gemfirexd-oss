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

//import util.TestHelper;
import java.util.*;
import perffmwk.*;
import hydra.*;

/**
 *  Contains static methods suitable for use as terminators.  All terminators
 *  should be kept brief for performance runs.
 *  <p>
 *  A <i>warmup</i> terminator is a public static boolean method that returns
 *  true when a client thread is considered to be "warmed up".  It is executed
 *  with a configurable frequency until it returns true, after which it is no
 *  longer invoked.
 *  <p>
 *  A <i>task</i> terminator is a public static boolean method that returns true
 *  when a client thread is considered to be "done" with a task.  It is
 *  executed with a configurable frequency until it returns true, at which time
 *  the client thread issues a {@link hydra.StopSchedulingTaskOnClientOrder} to
 *  stop work on the task and prevent being scheduled with the task in future.
 *  <p>
 *  A <i>batch</i> terminator is a public static void method that is executed
 *  by a client thread at the end of each batch of operations.
 *
 *  @see cacheperf.CachePerfPrms
 */

public class Terminators {

  //----------------------------------------------------------------------------
  //  Batch terminators
  //----------------------------------------------------------------------------

  /**
   * Returns true when a client has done the operation for {@link
   * CachePerfPrms#batchSeconds} seconds.
   *
   * @author lises
   * @since 5.0
   */
  public static boolean terminateOnBatchSeconds(CachePerfClient c) {
    if (c.batchSeconds == -1) {
      String s = BasePrms.nameForKey(CachePerfPrms.batchSeconds) + " is not set";
      throw new HydraConfigException(s);
    }
    if (System.currentTimeMillis() >= c.batchStartTime + c.batchSeconds * 1000) {
      if ( CachePerfClient.log().fineEnabled() ) {
        Log.getLogWriter().fine("Completed " + c.batchSeconds + " batch seconds");
      }
      return true;
    }
    return false;
  }

  /**
   *  Returns true when a client has done {@link CachePerfPrms#batchSize}
   *  of the operation.
   */
  public static boolean terminateOnBatchSize( CachePerfClient c ) {
    if ( c.batchSize == -1 ) {
      throw new HydraConfigException( BasePrms.nameForKey( CachePerfPrms.batchSize ) + " is not set" );
    }
    if ( c.batchCount >= c.batchSize ) {
      if ( CachePerfClient.log().fineEnabled() ) {
        Log.getLogWriter().fine( "Completed " + c.batchCount + " batch iterations" );
      }
      return true;
    }
    return false;
  }
  
  //----------------------------------------------------------------------------
  //  Warmup and task terminators
  //----------------------------------------------------------------------------

  /**
   * Returns true when a client has done the operation for {@link
   * CachePerfPrms#trimSeconds} seconds.
   *
   * @author lises
   * @since 5.0
   */
  public static boolean terminateOnTrimSeconds(CachePerfClient c) {
    if (c.trimSeconds == -1) {
      String s = BasePrms.nameForKey(CachePerfPrms.trimSeconds) + " is not set";
      throw new HydraConfigException(s);
    }
    long elapsed = System.currentTimeMillis() - c.startTime;
    if (elapsed >= c.trimSeconds * 1000) {
      Log.getLogWriter().info("Completed " + elapsed/1000 + " trim seconds");
      return true;
    }
    return false;
  }

  /**
   * Returns true when a client has done the operation for {@link
   * CachePerfPrms#trimSeconds} seconds.  Signals others using
   * {@link TaskSyncBlackboard#trimSignal}.
   *
   * @author lises
   * @since 5.0
   */
  public static boolean terminateAndSignalOnTrimSeconds(CachePerfClient c) {
    boolean terminate = terminateOnTrimSeconds(c);
    if (terminate) {
      TaskSyncBlackboard.getInstance().getSharedCounters()
                        .increment(TaskSyncBlackboard.trimSignal);
    }
    return terminate;
  }

  /**
   * Returns true when a client has done the operation for {@link
   * CachePerfPrms#workSeconds} seconds after warmup is completed.
   *
   * @author lises
   * @since 5.0
   */
  public static boolean terminateOnWorkSeconds(CachePerfClient c) {
    if (c.workSeconds == -1) {
      String s = BasePrms.nameForKey(CachePerfPrms.workSeconds) + " is not set";
      throw new HydraConfigException(s);
    }
    if (c.warmupTime != -1) {
      long elapsed = (System.currentTimeMillis() - c.startTime) - c.warmupTime;
      if (elapsed >= c.workSeconds * 1000) {
        String s = "Completed " + elapsed/1000 + " work seconds after "
                 + c.warmupTime/1000 + " warmup seconds";
        if (c.syncSleepMs > 0) {
          s += " (including syncSleepMs=" + c.syncSleepMs + ")";
        }
        Log.getLogWriter().info(s);
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true when a client has done the operation for {@link
   * CachePerfPrms#trimSeconds} plus {@link CachePerfPrms#workSeconds}.
   *
   * @author lises
   * @since 5.0
   */
  public static boolean terminateOnTotalSeconds(CachePerfClient c) {
    if (c.trimSeconds == -1) {
      String s = BasePrms.nameForKey(CachePerfPrms.trimSeconds) + " is not set";
      throw new HydraConfigException(s);
    }
    if (c.workSeconds == -1) {
      String s = BasePrms.nameForKey(CachePerfPrms.workSeconds) + " is not set";
      throw new HydraConfigException(s);
    }
    long elapsed = System.currentTimeMillis() - c.startTime;
    if (elapsed >= (c.trimSeconds + c.workSeconds) * 1000) {
      String s = "Completed " + elapsed/1000 + " total seconds";
      if (c.syncSleepMs > 0) {
        s += " (including syncSleepMs=" + c.syncSleepMs + ")";
      }
      Log.getLogWriter().info(s);
      return true;
    }
    return false;
  }

  /**
   *  Returns true when a client has done {@link CachePerfPrms#trimIterations}
   *  of the operation.
   */
  public static boolean terminateOnTrimIterations( CachePerfClient c ) {
    if ( c.trimIterations == -1 ) {
      throw new HydraConfigException( BasePrms.nameForKey( CachePerfPrms.trimIterations ) + " is not set" );
    }
    if ( c.count >= c.trimIterations ) {
      Log.getLogWriter().info( "Completed " + c.count + " trim iterations" );
      return true;
    }
    return false;
  }
  
  /** 
   *  Returns true when a client has done {@link CachePerfPrms#workIterations}
   *  of the operation after warmup is completed.
   */
  public static boolean terminateOnWorkIterations( CachePerfClient c ) {
    if ( c.workIterations == -1 ) {
      throw new HydraConfigException( BasePrms.nameForKey( CachePerfPrms.workIterations ) + " is not set" );
    }
    if ( ( c.warmupCount != -1 ) && ( c.count >= c.workIterations + c.warmupCount ) ) {
      Log.getLogWriter().info( "Completed " + c.workIterations + " work iterations after " + c.warmupCount + " warmup iterations" );
      return true;
    }
    return false;
  }

  /**
   *  Returns true when a client has done {@link CachePerfPrms#trimIterations}
   *  plus {@link CachePerfPrms#workIterations} of the operation.
   */
  public static boolean terminateOnTotalIterations( CachePerfClient c ) {
    if ( c.trimIterations == -1 ) {
      throw new HydraConfigException( BasePrms.nameForKey( CachePerfPrms.trimIterations ) + " is not set" );
    }
    if ( c.workIterations == -1 ) {
      throw new HydraConfigException( BasePrms.nameForKey( CachePerfPrms.workIterations ) + " is not set" );
    }
    if ( c.count >= c.trimIterations + c.workIterations ) {
      Log.getLogWriter().info( "Completed " + c.count + " total iterations" );
      return true;
    }
    return false;
  }

  /**
   *  Returns true when a client has done {@link CachePerfPrms#maxKeys} of the
   *  operation.
   */
  public static boolean terminateOnMaxKey( CachePerfClient c ) {
    if ( c.currentKey >= c.maxKeys ) {
      Log.getLogWriter().info( "Completed " + c.currentKey + " keys, surpasses max key" );
      return true;
    }
    return false;
  }

  /**
   *  Returns true when a client has done {@link CachePerfPrms#numOperations}
   *  operations.
   */
  public static boolean terminateOnNumOperations(CachePerfClient c) {
    if (c.numOperations == -1) {
      throw new HydraConfigException(BasePrms.nameForKey(CachePerfPrms.numOperations) + " is not set");
    }
    if (c.count >= c.numOperations) {
      Log.getLogWriter().info( "Completed " + c.count + " operations" );
      return true;
    }
    return false;
  }

  /**
   * Returns true when each stat archive has done {@link
   * CachePerfPrms#LRUEvictions}, based on checking lruEvictions stat, and at
   * least {@link CachePerfPrms#LRUEvictionsMinWaitSec} has passed.
   * @throws CachePerfException if this does not occur within
   *                            {@link CachePerfPrms#LRUEvictionsMaxWaitSec}.
   */
  public static boolean terminateOnLRUEvictions( CachePerfClient c ) {
    if ( CachePerfClient.log().fineEnabled() ) {
       CachePerfClient.log().fine( "Checking termination on LRU evictions" );
    }
    int lruEvictions = CachePerfPrms.LRUEvictions();
    int lruEvictionsMinWaitSec = CachePerfPrms.LRUEvictionsMinWaitSec();
    int lruEvictionsMaxWaitSec = CachePerfPrms.LRUEvictionsMaxWaitSec();
    if (lruEvictionsMaxWaitSec > 0 &&
        lruEvictionsMaxWaitSec <= lruEvictionsMinWaitSec) {
      String s = BasePrms.nameForKey(CachePerfPrms.LRUEvictionsMaxWaitSec)
               + "(" + lruEvictionsMaxWaitSec + ")" + " is less than "
               + BasePrms.nameForKey(CachePerfPrms.LRUEvictionsMinWaitSec)
               + "(" + lruEvictionsMinWaitSec + ")";
      throw new HydraConfigException(s);
    }
    boolean result = checkStat("LRUStatistics", "lruEvictions",
                                lruEvictions, false);
    long now = System.currentTimeMillis();
    if (result && lruEvictionsMinWaitSec != 0) {
      // make sure we wait the minimum time
      if (lruEvictionsMinTime == 0) {
        synchronized (Terminators.class) {
          if (lruEvictionsMinTime == 0) {
            lruEvictionsMinTime = now + lruEvictionsMinWaitSec * 1000;
          }
        }
      }
      if (now < lruEvictionsMinTime) {
        result = false;
      }
    }
    else if (!result && lruEvictionsMaxWaitSec != 0) {
      // make sure we wait no more than the maximum time
      if (lruEvictionsMaxTime == 0) {
        synchronized (Terminators.class) {
          if (lruEvictionsMaxTime == 0) {
            lruEvictionsMaxTime = now + lruEvictionsMaxWaitSec * 1000;
          }
        }
      }
      if (now > lruEvictionsMaxTime) {
        String s = "Failed to evict "
                 + BasePrms.nameForKey(CachePerfPrms.LRUEvictions)
                 + "(" + lruEvictions + ")" + " entries within "
                 + BasePrms.nameForKey(CachePerfPrms.LRUEvictionsMaxWaitSec)
                 + "(" + lruEvictionsMaxWaitSec + ")" + " seconds";
        throw new CachePerfException(s);
      }
    }
    return result;
  }
  private static long lruEvictionsMinTime = 0;
  private static long lruEvictionsMaxTime = 0;

  /**
   * Returns true when each stat archive has done {@link
   * CachePerfPrms#numDestroys}, based on checking destroys stat, and at
   * least {@link CachePerfPrms#numDestroysMinWaitSec} has passed.
   * @throws CachePerfException if this does not occur within
   *                            {@link CachePerfPrms#numDestroysMaxWaitSec}.
   */
  public static boolean terminateOnNumDestroys( CachePerfClient c ) {
    if ( CachePerfClient.log().fineEnabled() ) {
       CachePerfClient.log().fine( "Checking termination on num destroys" );
    }
    int numDestroys = CachePerfPrms.numDestroys();
    int numDestroysMinWaitSec = CachePerfPrms.numDestroysMinWaitSec();
    int numDestroysMaxWaitSec = CachePerfPrms.numDestroysMaxWaitSec();
    if (numDestroysMaxWaitSec > 0 &&
        numDestroysMaxWaitSec <= numDestroysMinWaitSec) {
      String s = BasePrms.nameForKey(CachePerfPrms.numDestroysMaxWaitSec)
               + "(" + numDestroysMaxWaitSec + ")" + " is less than "
               + BasePrms.nameForKey(CachePerfPrms.numDestroysMinWaitSec)
               + "(" + numDestroysMinWaitSec + ")";
      throw new HydraConfigException(s);
    }
    boolean result = checkStat("CachePerfStats", "destroys", numDestroys, true);
    long now = System.currentTimeMillis();
    if (result && numDestroysMinWaitSec != 0) {
      // make sure we wait the minimum time
      if (numDestroysMinTime == 0) {
        synchronized (Terminators.class) {
          if (numDestroysMinTime == 0) {
            numDestroysMinTime = now + numDestroysMinWaitSec * 1000;
          }
        }
      }
      if (now < numDestroysMinTime) {
        result = false;
      }
    }
    else if (!result && numDestroysMaxWaitSec != 0) {
      // make sure we wait no more than the maximum time
      if (numDestroysMaxTime == 0) {
        synchronized (Terminators.class) {
          if (numDestroysMaxTime == 0) {
            numDestroysMaxTime = now + numDestroysMaxWaitSec * 1000;
          }
        }
      }
      if (now > numDestroysMaxTime) {
        String s = "Failed to evict "
                 + BasePrms.nameForKey(CachePerfPrms.numDestroys)
                 + "(" + numDestroys + ")" + " entries within "
                 + BasePrms.nameForKey(CachePerfPrms.numDestroysMaxWaitSec)
                 + "(" + numDestroysMaxWaitSec + ")" + " seconds";
        throw new CachePerfException(s);
      }
    }
    return result;
  }
  private static long numDestroysMinTime = 0;
  private static long numDestroysMaxTime = 0;

  /**
   *  Checks that a given stat has a minimum getMax() value in each stat archive.
   *
   *  @param statType The name of the statistics type to check.
   *  @param statName The name of the stat to check.
   *  @param minAcceptable The minimum value acceptable for the maximum of the stat.
   *  @param exactMatch Determines if the stat type should be an exact match (to avoid
   *         confusion between the product's CachePerfStats and the framework's 
   *         cacheperf.CachePerfStats.
   *
   *  @returns true if each stat archive has reached the minAcceptable value of the
   *                given stat, false otherwise
   */
  private static boolean checkStat( String statType, String statName, int minAcceptable, boolean exactMatch) {
    String spec = "* " // search all archives
                  + statType + " " 
                  + "* " // match all instances
                  + statName + " " 
                  + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                  + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE + " "
                  + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    final int maxTries = 500; // workaround for bug 30288
    String errStr = null;
    for (int tryCount = 1; tryCount <= maxTries; tryCount++) {
       List aList = null;
       try {
          aList = PerfStatMgr.getInstance().readStatistics(spec, exactMatch);
       } catch (ArrayIndexOutOfBoundsException e) {
          errStr = e.toString();
          continue;
       }
       if (aList == null) {
         throw new PerfStatException("Statistics not found: " + spec);
       }
       boolean allArchivesMeetCriteria = true;
       StringBuffer aStr = new StringBuffer();
       for (int i = 0; i < aList.size(); i++) {
          PerfStatValue stat = (PerfStatValue)aList.get(i);
          double currValue = stat.getMax();
          aStr.append(statName + ": " + stat + "\n");
          if (currValue < minAcceptable) { 
             allArchivesMeetCriteria = false;
             break;
          }
      }
//   Log.getLogWriter().info(aStr.toString());
      if (allArchivesMeetCriteria) {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine("Warmup criteria has been met in " + tryCount + " stat reads; minimum acceptable value of the max is " + minAcceptable + ": " + aStr);
         }
       }
       return allArchivesMeetCriteria;
    }
    throw new CachePerfException("Could not get stats in " + maxTries + " attempts; " +
                   errStr);
  }

  /**
   *  Returns true when the shared counter in {@link TaskSyncBlackboard.signal} is non-zero.
   */
  public static boolean terminateOnSignal( CachePerfClient c ) {
    return TaskSyncBlackboard.getInstance().getSharedCounters().read( TaskSyncBlackboard.signal ) != 0;
  }

  /**
   *  Returns true when the shared counter in {@link getInitialImage.InitImageBB.getInitialImagesInProgress} is non-zero.
   */
  public static boolean terminateOnGetInitialImageInProgress( CachePerfClient c ) {
    return getInitialImage.InitImageBB.getBB().getSharedCounters().read( getInitialImage.InitImageBB.getInitialImageInProgress) != 0;
  }

  /**
   *  Returns true when the shared counter in {@link getInitialImage.InitImageBB.getInitialImagesCompleted} is non-zero.
   */
  public static boolean terminateOnGetInitialImageComplete( CachePerfClient c ) {
    return getInitialImage.InitImageBB.getBB().getSharedCounters().read( getInitialImage.InitImageBB.getInitialImageComplete) != 0;
  }
}
