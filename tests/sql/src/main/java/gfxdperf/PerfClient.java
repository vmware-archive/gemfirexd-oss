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

import gfxdperf.PerfPrms.KeyAllocation;
import gfxdperf.terminators.AbstractTerminator;
import gfxdperf.terminators.TerminatorFactory;
import gfxdperf.terminators.TerminatorFactory.TerminatorName;
import hydra.*;
import hydra.blackboard.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import perffmwk.*;
import util.TestHelper;

/**
 * Base client for measuring GFXD performance. Contains useful helper methods
 * and performance test framework support.
 */
public class PerfClient {

  private static HydraThreadLocal localtask = new HydraThreadLocal();
  private static HydraThreadLocal localbarriers = new HydraThreadLocal();
  private static HydraThreadLocal localterminator = new HydraThreadLocal();
  private static HydraThreadLocal localstatistics = new HydraThreadLocal();
  private static HydraThreadLocal localhistogram = new HydraThreadLocal();
  private static HydraThreadLocal localcurrentkey = new HydraThreadLocal();
  private static HydraThreadLocal localrng = new HydraThreadLocal();
  private static HydraThreadLocal localkeygenerator = new HydraThreadLocal();

  public TestTask task;
  public Map<String,Integer> barriers;
  public AbstractTerminator terminator;
  public PerfStats statistics;
  public HistogramStats histogram;
  public int currentKey;
  public GsRandom rng;
  public Random keyGenerator;

  public KeyAllocation keyAllocation;
  public int keyAllocationChunkSize;
  public int maxKeys;
  public boolean isMainWorkload;

  public int jid;
  public int tid;
  public int tgid;
  public int ttgid;
  public int sttgid;
  public int numWanSites;
  public int numThreads;

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

  /**
   * TASK to register the performance and clock skew statistics.
   */
  public static void openStatisticsTask() {
    PerfClient c = new PerfClient();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }

  private void openStatistics() {
    if (this.statistics == null) {
      this.statistics = PerfStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
  }

  /**
   * TASK to unregister the performance and clock skew statistics objects.
   */
  public static void closeStatisticsTask() {
    PerfClient c = new PerfClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }

  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.statistics != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.statistics.close();
    }
  }

//------------------------------------------------------------------------------
// Key support
//------------------------------------------------------------------------------

  protected int getNextKey() {
    int key;
    switch (this.keyAllocation) {
      case SameKey:
        key = 0;
        break;
      case SameKeys:
        if (this.currentKey == -1) {
          key = 0;
        } else {
          key = this.currentKey + 1;
        }
        break;
      case SameKeysWrap:
        if (this.currentKey == -1) {
          key = 0;
        } else {
          key = (this.currentKey + 1) % this.maxKeys;
        }
        break;
      case SameKeysRandomWrap:
        key = this.rng.nextInt(0, this.maxKeys - 1);
        break;
      case OwnKey:
        if (this.currentKey == -1) {
          checkSufficientKeys();
          key = this.ttgid;
        } else {
          key = this.currentKey;
        }
        break;
      case OwnKeys:
        if (this.currentKey == -1) {
          checkSufficientKeys();
          key = this.ttgid;
        } else {
          key = this.currentKey + this.numThreads;
        }
        break;
      case OwnKeysWrap:
        if (this.currentKey == -1) {
          checkSufficientKeys();
          key = this.ttgid;
        } else {
          key = this.currentKey + this.numThreads;
        }
        if (key >= this.maxKeys) {
          key = this.ttgid;
        }
        break;
      case OwnKeysRandomWrap:
        if (this.currentKey == -1) {
          checkSufficientKeys();
        }
        int numKeys = (int) Math.ceil(this.maxKeys / this.numThreads);
        if (this.ttgid >= this.maxKeys % this.numThreads) {
          --numKeys;
        }
        key = this.ttgid + this.numThreads * this.rng.nextInt(0, numKeys);
        break;
      case OwnKeysChunked:
        if (this.currentKey == -1) {
          checkSufficientKeys();
          key = this.ttgid * this.keyAllocationChunkSize;
        } else if ((this.currentKey + 1) % this.keyAllocationChunkSize == 0) {
          key = this.currentKey + 1
              + (this.numThreads - 1) * this.keyAllocationChunkSize;
        } else {
          key = this.currentKey + 1;
        }
        break;
      case OwnKeysChunkedRandomWrap:
        if (this.currentKey == -1) {
          checkSufficientKeys();
          PerfPrms.checkOwnKeysChunkedRandomWrap(this.maxKeys,
                                                 this.keyAllocationChunkSize);
        }
        if ((this.currentKey + 1) % this.keyAllocationChunkSize == 0) {
          // first pick a random "own" chunk number
          int numChunks = this.maxKeys/this.keyAllocationChunkSize;
          int numChunksPerThread = (int)Math.ceil(numChunks/this.numThreads);
          if (this.ttgid >= numChunks % this.numThreads) {
            --numChunksPerThread;
          }
          int chunk = this.ttgid + this.numThreads
                                 * this.rng.nextInt(0, numChunksPerThread);
          key = chunk * this.keyAllocationChunkSize;
        } else {
          // continue this chunk
          key = this.currentKey + 1;
        }
        break;
      case Zipfian:
        key = this.keyGenerator.nextInt();
        break;
      case ScrambledZipfian:
        key = this.keyGenerator.nextInt();
        break;
      default:
        throw new HydraInternalException("Should not happen");
    }
    this.currentKey = key;
    return key;
  }

  private void checkSufficientKeys() {
    if (this.numThreads > this.maxKeys / this.keyAllocationChunkSize) {
      throw new HydraConfigException(
        this.maxKeys + " keys are not enough for " +
        this.numThreads + " threads to have their own keys (or chunk of keys)"
      );
    }
  }

//------------------------------------------------------------------------------
// Synchronization
//------------------------------------------------------------------------------

  /**
   * Waits for all threads in all thread groups for the current task to meet at
   * a barrier. Useful for synchronizing clients across JVMs after warming up
   * and before starting a timing run, for example.
   */
  public void sync() {
    String taskId = this.task.getReceiver() + "_" + this.task.getSelector()
                  + "_" + this.task.getTaskIndex();
    Integer barrierId = this.barriers.containsKey(taskId)
                      ? this.barriers.get(taskId) + 1
                      : 1;
    String barrierName = taskId + "_" + barrierId;
    int parties = numThreads();
    Log.getLogWriter().info("syncing at barrier " + barrierName + " for "
                           + parties + " parties");
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(parties, barrierName);
    barrier.await();
    this.barriers.put(taskId, barrierId);
    int syncSleepMs = PerfPrms.getSyncSleepMs();
    if (syncSleepMs > 0) {
      Log.getLogWriter().info("sleeping " + syncSleepMs + " ms after sync");
      MasterController.sleepForMs(syncSleepMs);
    }
    Log.getLogWriter().info("done syncing");
  }
  
  public static void dumpHeapTask() {
    PerfClient c = new PerfClient();
    c.initLocalVariables();
    c.dumpHeap();
  }

  private void dumpHeap() {
    if (this.ttgid == 0) {
      Log.getLogWriter().severe(ProcessMgr.fgexec("sh heapdumprun.sh", 600));
    }
  }

//------------------------------------------------------------------------------
// Initialization
//------------------------------------------------------------------------------

  protected void initialize() {
    initLocalVariables();
    initLocalParameters();
    initHydraThreadLocals();
  }

  protected void initLocalVariables() {
    this.jid = jid();
    this.tid = tid();
    this.tgid = tgid();
    this.ttgid = ttgid();
    this.sttgid = sttgid();
    this.numWanSites = numWanSites();
    this.numThreads = numThreads();
    com.gemstone.gemfire.internal.Assert.assertTrue(this.numThreads > 0,
                                  "Have " + this.numThreads + " threads");
  }

  protected void initLocalParameters() {
    this.isMainWorkload = PerfPrms.isMainWorkload();
    this.keyAllocation = PerfPrms.getKeyAllocation();
    this.keyAllocationChunkSize = PerfPrms.getKeyAllocationChunkSize();
    this.maxKeys = PerfPrms.getMaxKeys();
  }

  /**
   * Used to init hydra thread locals at the beginning of a task execution.
   */
  public void initHydraThreadLocals() {
    // read-only
    this.task = getTask();
    this.terminator = getTerminator();
    this.rng = getRNG();
    this.keyGenerator = getKeyGenerator();

    // updated in special tasks
    this.barriers = getBarriers();
    this.statistics = getStatistics();
    this.histogram = getHistogram(); // derived

    // updated in tasks
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    this.currentKey = getCurrentKey();
  }

  /**
   * Used to update hydra thread locals at the end of a task execution.
   */
  public void updateHydraThreadLocals() {
    setTerminator(this.terminator);
    setBarriers(this.barriers);
    setStatistics(this.statistics);
    setHistogram(this.histogram);
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    setCurrentKey(this.currentKey);
  }

  /**
   * Used to reset hydra thread locals when the test task id changes.
   */
  protected void resetHydraThreadLocals(boolean resetKeys) {
    localterminator.set(null);
    //localbarriers?
    if (resetKeys) {
      localcurrentkey.set(null);
    }
    HistogramStats h = (HistogramStats)localhistogram.get();
    if (h != null) {
      h.close();
      localhistogram.set(null);
    }
  }

  /**
   * Gets the current task.
   */
  protected TestTask getTask() {
    TestTask oldTask = (TestTask) localtask.get();
    TestTask newTask = RemoteTestModule.getCurrentThread().getCurrentTask();
    if (oldTask == null) {
      localtask.set(newTask);
    } else if (! oldTask.equals(newTask)) {
      localtask.set(newTask);
      boolean resetKeysAfter = PerfPrms.resetKeysAfterTaskEnds(oldTask);
      boolean resetKeysBefore = PerfPrms.resetKeysBeforeTaskStarts(newTask);
      resetHydraThreadLocals(resetKeysAfter || resetKeysBefore);
    }
    return newTask;
  }

  /**
   * Gets the per-thread map of barrier names and times used.
   */
  protected Map<String,Integer> getBarriers() {
    Map m = (Map<String,Integer>)localbarriers.get();
    if (m == null) {
      m = new HashMap<String,Integer>();
      localbarriers.set(m);
    }
    return (Map<String,Integer>)localbarriers.get();
  }

  /**
   * Sets the per-thread barriers instance.
   */
  protected void setBarriers(Map<String,Integer> m) {
    localbarriers.set(m);
  }

  /**
   * Gets the per-thread performance statistics instance.
   */
  protected PerfStats getStatistics() {
    return (PerfStats)localstatistics.get();
  }

  /**
   * Sets the per-thread performance statistics instance.
   */
  protected void setStatistics(PerfStats stats) {
    localstatistics.set(stats);
  }

  /**
   * Gets the per-thread histogram statistics instance.
   */
  protected HistogramStats getHistogram() {
    HistogramStats h = (HistogramStats)localhistogram.get();
    if (h == null && HistogramStatsPrms.enable()) {
      String instanceName = "junk";
      Log.getLogWriter().info("Opening histogram for " + instanceName);
      h = HistogramStats.getInstance(instanceName);
      Log.getLogWriter().info("Opened histogram for " + instanceName);
      localhistogram.set(h);
    }
    return h;
  }

  /**
   * Sets the per-thread histogram instance.
   */
  protected void setHistogram(HistogramStats h) {
    localhistogram.set(h);
  }

  /**
   * Gets the terminator for a thread's workload.
   */
  protected AbstractTerminator getTerminator() {
    AbstractTerminator t = (AbstractTerminator)localterminator.get();
    if (t == null) {
      TerminatorName terminatorName = PerfPrms.getTerminatorName();
      if (terminatorName != null) {
        t = TerminatorFactory.create(terminatorName);
        localterminator.set(t);
      }
    }
    return t;
  }

  /**
   * Sets the terminator for a thread's workload.
   */
  protected void setTerminator(AbstractTerminator t) {
    localterminator.set(t);
  }

  /**
   * Gets the current key for a thread's workload.
   */
  protected int getCurrentKey() {
    Integer n = (Integer) localcurrentkey.get();
    if (n == null) {
      n = Integer.valueOf(-1);
      localcurrentkey.set(n);
    }
    return n.intValue();
  }

  /**
   * Sets the current key for a thread's workload.
   */
  protected void setCurrentKey(int n) {
    localcurrentkey.set(Integer.valueOf(n));
  }

  /**
   * Gets the random number generator.
   */
  protected GsRandom getRNG() {
    GsRandom r = (GsRandom) localrng.get();
    if (r == null) {
      r = TestConfig.getInstance().getParameters().getRandGen();
      localrng.set(r);
    }
    return r;
  }

  /**
   * Gets the key generator for zipfian and scrambledZipfian distribution
   */
  protected Random getKeyGenerator() {
    Random keyGenerator = (Random)localkeygenerator.get();
    if (keyGenerator == null) {
      if (this.keyAllocation == KeyAllocation.Zipfian) {
        keyGenerator = new ZipfianGenerator(this.maxKeys);
      } else if (this.keyAllocation == KeyAllocation.ScrambledZipfian) {
        keyGenerator = new ScrambledZipfianGenerator(this.maxKeys);
      }
      localkeygenerator.set(keyGenerator);
    }
    return keyGenerator;
  }
  
//------------------------------------------------------------------------------
// Miscellaneous convenience methods
//------------------------------------------------------------------------------

  /**
   * Gets the client's JVM thread id. Exactly one logical hydra thread in the
   * this JVM has a given jid. The jids are numbered consecutively starting
   * from 0.  Assumes that all threads in this JVM are in the same threadgroup.
   */
  protected int jid() {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("jid=" + tid() % RemoteTestModule.getMyNumThreads());
    }
    return tid() % RemoteTestModule.getMyNumThreads();
  }

  /**
   * Gets the client's hydra thread id. Exactly one logical hydra thread
   * in the whole test has a given tid. The tids are numbered consecutively
   * starting from 0.
   */
  protected int tid() {
    return RemoteTestModule.getCurrentThread().getThreadId();
  }

  /**
   * Gets the client's hydra threadgroup id. Exactly one logical hydra thread
   * in the threadgroup containing this thread has a given tgid. The tgids are
   * numbered consecutively starting from 0.
   */
  protected int tgid() {
    return RemoteTestModule.getCurrentThread().getThreadGroupId();
  }

  /**
   * Gets the client's hydra task threadgroup id.  Exactly one logical hydra
   * thread in all of the threadgroups assigned to the current task has a
   * given ttgid.  The ttgids are numbered consecutively starting from 0.
   */
  protected int ttgid() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    return task.getTaskThreadGroupId(tgname, tgid());
  }

  /**
   * Gets the client's hydra site-specific task threadgroup id.  Exactly one
   * logical hydra thread in all of the threads in the threadgroups assigned to
   * the current task threads that are in the wan site of this thread has a
   * given sttgid.  The sttgids for each wan site are numbers consecutively
   * starting from 0.
   * <p>
   * Assumes client names are of the form *_site_*, where site is the site
   * number.  If no site is found, then this method returns the {@link #ttgid}.
   */
  protected int sttgid() {
    List<List<String>> mappings = RemoteTestModule.getClientMapping();
    if (mappings == null) {
      String s = "Client mapping not found";
      throw new HydraRuntimeException(s);
    }

    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    String arr[] = clientName.split("_");
    if (arr.length != 3) {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("sttgid: single site, using ttgid=" + ttgid());
      }
      return ttgid(); // assume single site
    }
    String localsite = arr[1];
    String localthreadname = Thread.currentThread().getName();
    Vector threadgroups = RemoteTestModule.getCurrentThread().getCurrentTask()
                                          .getThreadGroupNames();
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("sttgid: matching " + localthreadname
         + " for site=" + localsite + " from threadgroups=" + threadgroups);
    }

    int count = 0;
    for (List<String> mapping : mappings) {
      String threadgroup = mapping.get(0);
      String threadname = mapping.get(1);
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("sttgid: candidate threadgroup=" + threadgroup
                  + " threadname= " + threadname);
      }
      if (threadgroups.contains(threadgroup)) {
        // candidate thread is in the task thread group
        String carr[] = threadname.split("_");
        if (carr.length == 8) {
          if (localthreadname.startsWith(threadname)) {
            // this is the calling thread
            if (Log.getLogWriter().fineEnabled()) {
              Log.getLogWriter().fine("sttgid: returning match for "
                        + localthreadname + " as " + count);
            }
            return count;
          }
          String site = carr[5];
          if (site.equals(localsite)) {
            // candidate thread is in this site
            ++count;
            if (Log.getLogWriter().fineEnabled()) {
              Log.getLogWriter().fine("sttgid: counting match " + count
                                     + "=" + threadname);
            }
          } else {
            if (Log.getLogWriter().fineEnabled()) {
              Log.getLogWriter().fine("sttgid: candidate rejected=" + mapping
                        + " invalid site=" + site);
            }
          }
        } else {
          String s = "Unexpected array size from " + threadname;
          throw new HydraRuntimeException(s);
        }
      } else {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine("sttgid: candidate rejected=" + mapping
                    + " invalid threadgroup=" + threadgroup);
        }
      }
    }
    String s = "Should not happen";
    throw new HydraInternalException(s);
  }

  /**
   * Gets the number of wan sites in the test.
   * <p>
   * Reads {@link PerfPrms#numWanSites}. If that returns 0 (not set),
   * assumes client names are of the form *_site_*, where site is the site
   * number.  If no site is found, then this method returns 1.
   */
  protected int numWanSites() {
    int maxSite = PerfPrms.getNumWanSites();
    if (maxSite != 0) {
      return maxSite;
    }
    // else compute it based on the client name
    Vector clientNames = TestConfig.getInstance().getClientNames();
    for (Iterator i = clientNames.iterator(); i.hasNext();) {
      String clientName = (String)i.next();
      String arr[] = clientName.split("_");
      if (arr.length != 3) {
        return 1; // assume single site
      }
      int site;
      try {
        site = Integer.parseInt(arr[1]);
      } catch (NumberFormatException e) {
        String s = clientName
                 + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
        throw new HydraRuntimeException(s, e);
      }
      maxSite = Math.max(site, maxSite);
    }
    if (maxSite == 0) {
      String s = "Should not happen";
      throw new HydraInternalException(s);
    }
    return maxSite;
  }

  /**
   * Gets the total number of threads eligible to run the current task.
   */
  public static int numThreads() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    int t = task.getTotalThreads();
    return t;
  }

  /**
   * Gets a stack trace for the given Throwable and return it as a String.
   */
  public static String getStackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw, true));
    return sw.toString();
  }

  /**
   * Returns the {@link hydra.HostDescription} for this hydra client.
   */
  public static HostDescription getHostDescription() {
    String clientName = RemoteTestModule.getMyClientName();
    ClientDescription cd = TestConfig.getInstance()
                                     .getClientDescription(clientName);
    return cd.getVmDescription().getHostDescription();
  }
}
