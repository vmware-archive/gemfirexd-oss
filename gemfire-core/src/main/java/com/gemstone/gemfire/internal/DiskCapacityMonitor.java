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

package com.gemstone.gemfire.internal;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.DirectoryHolder;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TObjectLongHashMap;

/**
 * @author kneeraj
 * 
 */
public class DiskCapacityMonitor {

  private final LogWriterI18n logger;

  private File logdir;

  private final TObjectLongHashMap lastWarnTimeMillis;

  private long lastSampledMillis;

  private static final int DISKSPACE_WARN_INTERVAL = Integer.getInteger(
      "gemfire.DISKSPACE_WARNING_INTERVAL", 10000);

  private DiskCapacityMonitor(InternalDistributedSystem sys) {
    logger = sys.getLogWriterI18n();
    File logfile = sys.getConfig().getLogFile();
    if (logfile != null) {
      logdir = logfile.getParentFile();
      if (logdir == null) {
        // assuming curr directory is where the logging
        // is happening
        logdir = new File(".");
      }
    }
    this.lastWarnTimeMillis = new TObjectLongHashMap();
  }

  private static volatile DiskCapacityMonitor instance = null;

  public static DiskCapacityMonitor getInstance() {
    DiskCapacityMonitor inst = instance;
    if (inst != null) {
      return inst;
    }
    synchronized (DiskCapacityMonitor.class) {
      inst = instance;
      if (inst == null) {
        InternalDistributedSystem sys = InternalDistributedSystem
            .getConnectedInstance();
        if (sys != null) {
          DiskCapacityMonitor.instance = inst = new DiskCapacityMonitor(sys);
        }
      }
      return inst;
    }
  }

  public static void clearInstance() {
    synchronized (DiskCapacityMonitor.class) {
      instance = null;
    }
  }

  private static long HUNDRED_MB = 100 * 1024 * 1024;

  private static long FIFTY_MB = 50 * 1024 * 1024;

  public void checkAvailableSpace() {

    long currTimeMillis = System.currentTimeMillis();

    long gap = (currTimeMillis - this.lastSampledMillis);

    if (gap < 5000) {
      return;
    }
    this.lastSampledMillis = currTimeMillis;

    ArrayList<String> dirPaths = new ArrayList<String>();
    if (this.logdir != null) {
      String logDirPath = this.logdir.getAbsolutePath();
      if (logDirPath != null) {
        if (NativeCalls.getInstance().isOnLocalFileSystem(logDirPath)) {
          long lastWarnTime = this.lastWarnTimeMillis.get(logDirPath);
          long interval = currTimeMillis - lastWarnTime;
          if (interval > DISKSPACE_WARN_INTERVAL) {
            long usablespace = this.logdir.getUsableSpace();
            if (usablespace < HUNDRED_MB) {
              long usableSpaceInMB = 0;
              if (usablespace != 0) {
                usableSpaceInMB = Math.round(usablespace / (1024 * 1024));
              }
              this.logger
                  .warning(LocalizedStrings.LOGDIR_FULL_WARNING, new Object[] {
                      this.logdir.getAbsolutePath(), usableSpaceInMB });
              this.lastWarnTimeMillis.put(logDirPath, currTimeMillis);
              dirPaths.add(logDirPath);
            }
          }
        }
      }
    }
    GemFireCacheImpl cache = null;
    try {
      cache = GemFireCacheImpl.getExisting();
    } catch (Exception e) {
      // ignore if the cache has become null or throws some exception then
      // return. no need to monitor
      // disk
      return;
    }
    if (cache != null) {
      Collection<DiskStoreImpl> stores = cache.listDiskStores();
      for (DiskStoreImpl dImpl : stores) {
        DirectoryHolder[] dirs = dImpl.getDirectories();
        long limit = calcLimit(dImpl);
        for (DirectoryHolder dh : dirs) {
          File d = dh.getDir();
          String path = d.getAbsolutePath();
          if (path != null
              && NativeCalls.getInstance().isOnLocalFileSystem(path)) {
            if (dirPaths.contains(path)) {
              continue;
            }
            long lastWarnTime = this.lastWarnTimeMillis.get(path);
            long interval = currTimeMillis - lastWarnTime;
            if (interval < DISKSPACE_WARN_INTERVAL) {
              continue;
            }
            long usablespace = d.getUsableSpace();
            if (usablespace < limit) {
              float usableSpaceInMB = 0;
              if (usablespace != 0) {
                usableSpaceInMB = Math
                    .round((usablespace / (1024f * 1024f)) * 1000) / 1000f;
              }
              this.logger.warning(
                  LocalizedStrings.DiskStoreImpl_DISK_FULL_WARNING,
                  new Object[] { d.getAbsolutePath(), dImpl.getName(),
                      usableSpaceInMB });
              this.lastWarnTimeMillis.put(path, currTimeMillis);
            }
          }
        }
      }
    }
  }

  private long calcLimit(DiskStoreImpl dImpl) {
    long limit = dImpl.getMaxOplogSizeInBytes();
    limit = Math.max(Math.round(1.15 * limit), FIFTY_MB);
    return limit;
  }
}
