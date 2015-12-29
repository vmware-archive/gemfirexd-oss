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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer.Compactor;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.persistence.soplog.ComponentLogWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.util.LogService;

/**
 * A singleton which schedules compaction of hoplogs owned by this node as primary and manages
 * executor of ongoing compactions. Ideally the number of pending request will not exceed the number
 * of buckets in the node as hoplog organizer avoids creating a new request if compaction on the
 * bucket is active. Moreover separate queues for major and minor compactions are maintained to
 * prevent long running major compactions from preventing minor compactions.
 */
public class HDFSCompactionManager {
  /*
   * Each hdfs store has its own concurrency configuration. Concurrency
   * configuration is used by compaction manager to manage threads. This member
   * holds hdsf-store to compaction manager mapping
   */
  private static final ConcurrentHashMap<String, HDFSCompactionManager> storeToManagerMap = 
                                        new ConcurrentHashMap<String, HDFSCompactionManager>();

  // hdfs store configuration used to initialize this instance
  HDFSStore storeConfig;
  
  // Executor for ordered execution of minor compaction requests.
  private final CompactionExecutor minorCompactor;
  // Executor for ordered execution of major compaction requests.
  private final CompactionExecutor majorCompactor;

  final ComponentLogWriter logger = ComponentLogWriter.getHoplogLogWriter("HDFSCompactionManager", LogService.logger());

  private HDFSCompactionManager(HDFSStore config) {
    this.storeConfig = config;
    // configure hdfs compaction manager
    HDFSCompactionConfig compactionConf = config.getHDFSCompactionConfig();
    
    int capacity = Integer.getInteger(HoplogConfig.COMPCATION_QUEUE_CAPACITY,
        HoplogConfig.COMPCATION_QUEUE_CAPACITY_DEFAULT);

    minorCompactor = new CompactionExecutor(compactionConf.getMaxThreads(),
        capacity, "MinorCompactor_" + config.getName());

    majorCompactor = new CompactionExecutor(
        compactionConf.getMajorCompactionMaxThreads(), capacity, "MajorCompactor_"
            + config.getName());

    minorCompactor.allowCoreThreadTimeOut(true);
    majorCompactor.allowCoreThreadTimeOut(true);
  }

  public static synchronized HDFSCompactionManager getInstance(HDFSStore config) {
    HDFSCompactionManager instance = storeToManagerMap.get(config.getName());
    if (instance == null) {
      instance = new HDFSCompactionManager(config);
      storeToManagerMap.put(config.getName(), instance);
    }
    
    return instance;
  }

  /**
   * Accepts compaction request for asynchronous compaction execution.
   * 
   * @param request
   *          compaction request with region and bucket id
   * @return true if the request is accepted, false if the compactor is overlaoded and there is a
   *         long wait queue
   */
  public synchronized Future<CompactionStatus> submitRequest(CompactionRequest request) {
    if (!request.isForced && request.compactor.isBusy(request.isMajor)) {
      if (logger.fineEnabled()) {
        fineLog("Compactor is busy. Ignoring ", request);
      }
      return null;
    }
    
    CompactionExecutor executor = request.isMajor ? majorCompactor : minorCompactor;
    
    try {
      return executor.submit(request);
    } catch (Throwable e) {
      if (e instanceof CompactionIsDisabled) {
        if (logger.finestEnabled()) {
          logger.finest(e.getMessage());
        }
      } else {
        logger.info(LocalizedStrings.ONE_ARG, "Compaction request submission failed", e);
      }
    }
    return null;
  }

  /**
   * Removes all pending compaction requests. Programmed for TESTING ONLY
   */
  public void reset() {
    minorCompactor.shutdownNow();
    majorCompactor.shutdownNow();
    HDFSCompactionManager.storeToManagerMap.remove(storeConfig.getName());
  }
  
  /**
   * Returns minor compactor. Programmed for TESTING AND MONITORING ONLY  
   */
  public ThreadPoolExecutor getMinorCompactor() {
    return minorCompactor;
  }

  /**
   * Returns major compactor. Programmed for TESTING AND MONITORING ONLY  
   */
  public ThreadPoolExecutor getMajorCompactor() {
    return majorCompactor;
  }
  
  /**
   * Contains important details needed for executing a compaction cycle.
   */
  public static class CompactionRequest implements Callable<CompactionStatus> {
    String regionFolder;
    int bucket;
    Compactor compactor;
    boolean isMajor;
    final boolean isForced;
    final boolean versionUpgrade;

    public CompactionRequest(String regionFolder, int bucket, Compactor compactor, boolean major) {
      this(regionFolder, bucket, compactor, major, false);
    }

    public CompactionRequest(String regionFolder, int bucket, Compactor compactor, boolean major, boolean isForced) {
      this(regionFolder, bucket, compactor, major, isForced, false);
    }

    public CompactionRequest(String regionFolder, int bucket, Compactor compactor, boolean major, boolean isForced, boolean versionUpgrade) {
      this.regionFolder = regionFolder;
      this.bucket = bucket;
      this.compactor = compactor;
      this.isMajor = major;
      this.isForced = isForced;
      this.versionUpgrade = versionUpgrade;
    }

    @Override
    public CompactionStatus call() throws Exception {
      HDFSStore store = compactor.getHdfsStore();
      if (!isForced) {
        // this is a auto generated compaction request. If auto compaction is
        // disabled, ignore this call.
        if (isMajor && !store.getHDFSCompactionConfig().getAutoMajorCompaction()) {
          LogWriterI18n logger = getLogger(regionFolder);
          if (logger.fineEnabled()) {
            logger.fine("Major compaction is disabled. Ignoring request");
          }
          return new CompactionStatus(bucket, false);
        } else if (!isMajor && !store.getHDFSCompactionConfig().getAutoCompaction()) {
          LogWriterI18n logger = getLogger(regionFolder);
          if (logger.fineEnabled()) {
            logger.fine("Minor compaction is disabled. Ignoring request");
          }
          return new CompactionStatus(bucket, false);
        }
      }

      // all hurdles passed, execute compaction now
      try {
        boolean status = compactor.compact(isMajor, versionUpgrade);
        return new CompactionStatus(bucket, status);
      } catch (IOException e) {
        LogWriterI18n logger = getLogger(regionFolder);
        logger.error(LocalizedStrings.HOPLOG_HDFS_COMPACTION_ERROR, bucket, e);
      }
      return new CompactionStatus(bucket, false);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + bucket;
      result = prime * result
          + ((regionFolder == null) ? 0 : regionFolder.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CompactionRequest other = (CompactionRequest) obj;
      if (bucket != other.bucket)
        return false;
      if (regionFolder == null) {
        if (other.regionFolder != null)
          return false;
      } else if (!regionFolder.equals(other.regionFolder))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "CompactionRequest [regionFolder=" + regionFolder + ", bucket="
          + bucket + ", isMajor=" + isMajor + ", isForced="+isForced+"]";
    }
  }

  /**
   * Helper class for creating named instances of comapction threads and managing compaction
   * executor. All threads wait infinitely
   */
  private class CompactionExecutor extends ThreadPoolExecutor implements ThreadFactory {
    final AtomicInteger count = new AtomicInteger(1);
    private String name;

    CompactionExecutor(int max, int capacity, String name) {
      super(max, max, 5, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(capacity));
      allowCoreThreadTimeOut(true);
      setThreadFactory(this);
      this.name = name;
    }
    
    private void throwIfStopped(CompactionRequest req, HDFSCompactionConfig config) {
      // check if compaction is enabled everytime. Alter may change this value
      // so this check is needed everytime
      boolean isEnabled = true;
      isEnabled = config.getAutoCompaction();
      if (req.isMajor) {
        isEnabled = config.getAutoMajorCompaction();
      }
      if (isEnabled || req.isForced) {
        return;
      }
      throw new CompactionIsDisabled(name + " is disabled");
    }

    private void throwIfPoolSizeChanged(CompactionRequest task, HDFSCompactionConfig config) {
      int threadCount = config.getMaxThreads();
      if (task.isMajor) {
        threadCount = config.getMajorCompactionMaxThreads();
      }
      
      if (getCorePoolSize() < threadCount) {
        setCorePoolSize(threadCount);
      } else if (getCorePoolSize() > threadCount) {
        setCorePoolSize(threadCount);
      }
      
      if (!task.isForced && getActiveCount() > threadCount) {
        // the number is active threads is more than new max pool size. Throw
        // error is this is system generated compaction request
        throw new CompactionIsDisabled(
            "Rejecting to reduce the number of threads for " + name
            + ", currently:" + getActiveCount() + " target:"
            + threadCount);
      }
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> task) {
      HDFSCompactionConfig config;
      config = HDFSCompactionManager.this.storeConfig.getHDFSCompactionConfig();
      
      throwIfStopped((CompactionRequest) task, config);
      throwIfPoolSizeChanged((CompactionRequest) task, config);
      
      if (logger.fineEnabled()) {
        fineLog("New:", task, " pool:", getPoolSize(), " active:", getActiveCount());
      }
      return super.submit(task);
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r, name + ":" + count.getAndIncrement());
      thread.setDaemon(true);
      if (logger.fineEnabled()) {
        fineLog("New thread:", name, " poolSize:", getPoolSize(),
            " active:", getActiveCount());
      }
      return thread;
    }
  }
  
  public static class CompactionIsDisabled extends RejectedExecutionException {
    private static final long serialVersionUID = 1L;
    public CompactionIsDisabled(String name) {
      super(name);
    }
  }
  
  private static ComponentLogWriter getLogger(String region) {
    return ComponentLogWriter.getHoplogLogWriter(region, LogService.logger());
  }
  
  private void fineLog(Object... strings) {
    if (logger.fineEnabled()) {
      StringBuffer sb = new StringBuffer();
      for (Object str : strings) {
        sb.append(str.toString());
      }
      logger.fine(sb.toString());
    }
  }
}
