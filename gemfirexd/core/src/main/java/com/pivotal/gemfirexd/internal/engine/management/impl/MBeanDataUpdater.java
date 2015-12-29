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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.shared.SystemProperties;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.diag.MemoryAnalyticsVTI;
import com.pivotal.gemfirexd.internal.engine.management.impl.TableMBeanBridge.MemoryAnalyticsData;

/**
 *
 * @author Abhishek Chaudhari
 * @since gfxd 1.0
 */
public abstract class MBeanDataUpdater<T> implements Runnable, Cleanable {
  protected Map<String, Updatable<T>>  updatables = new ConcurrentHashMap<String, Updatable<T>>();

  public MBeanDataUpdater() {}

  public void addUpdatable(String key, Updatable<T> updatable) {
    this.updatables.put(key, updatable);
  }

  public void removeUpdatable(String key) {
    this.updatables.remove(key);
  }

  @Override
  public void cleanUp() {
    this.updatables.clear();
  }

  public boolean isCacheRunning() {
    GemFireCacheImpl gemFireCache = Misc.getGemFireCacheNoThrow();

    return gemFireCache != null && !gemFireCache.isClosed();
  }

  protected void logFine(Exception e) {
    LogWriter logWriter = Misc.getCacheLogWriterNoThrow();
    if (logWriter != null && logWriter.fineEnabled()) {
      logWriter.fine(e);
    }
  }
  
  protected void logInfo(String str) {
    LogWriter logWriter = Misc.getCacheLogWriterNoThrow();
    if (logWriter != null) {
      logWriter.info(str);
    }
  }

  protected void logFiner(Exception e) {
    LogWriter logWriter = Misc.getCacheLogWriterNoThrow();
    if (logWriter != null && logWriter.finerEnabled()) {
      logWriter.finer(e);
    }
  }

  @Override
  public void run() {
    if (!this.updatables.isEmpty() && isCacheRunning()) {
      try {
        runUpdate();
      } catch (Exception e) {
//        e.printStackTrace();
        logFine(e); // don't propagate exception
      }
    }
  }

  protected abstract void runUpdate();
}

/**
 *
 * @author Abhishek Chaudhari
 * @since gfxd 1.0
 */
class MemberMBeanDataUpdater extends MBeanDataUpdater<GfxdMemberMBeanBridge> {
  public MemberMBeanDataUpdater() {}

  @Override
  protected void runUpdate() {
    Set<Entry<String, Updatable<GfxdMemberMBeanBridge>>> entries = updatables.entrySet();
    for (Entry<String, Updatable<GfxdMemberMBeanBridge>> entry : entries) {
      entry.getValue().update();
    }
  }
}

/**
 *
 * @author Abhishek Chaudhari
 * @since gfxd 1.0
 */
class TableMBeanDataUpdater extends MBeanDataUpdater<TableMBeanBridge> {
  private MemoryAnalyticsHolder memAnaHolder;
  private Random random;
  private int updateNumer;

  public TableMBeanDataUpdater() {
    this.random = new Random();
    this.updateNumer = 0;
    this.memAnaHolder = new MemoryAnalyticsHolder(new MemoryAnalyticsVTI(true));
  }

  @Override
  protected void runUpdate() {
    Updatable<TableMBeanBridge> updatable = null;
    // use same token for all table MemoryAnalyticsVTI updates.
    // Note this has to be different than updateNumer because this should change
    // & remain same only for each MemoryAnalyticsVTI.next() invocation
    // updateNumer changes for each update
    int updateToken = this.random.nextInt();

    try {
      // don't update MemoryAnalytics on each update
      if (Misc.initialDDLReplayDone()
          && (this.updateNumer % (this.memAnaHolder.updatesToSkip == 0 ? this.updateNumer: this.memAnaHolder.updatesToSkip)) == 0) {
        MemoryAnalyticsVTI memoryAnalyticsVTI = this.memAnaHolder.memAnaVTI;
        
        // gather column indices only the first time       
        this.memAnaHolder.init();

        // Now gather MemoryAnalytics
        while (memoryAnalyticsVTI.next()) {
          updatable = null;
          try {
            String tableName = memoryAnalyticsVTI.getString(this.memAnaHolder.tableNameIndex);
            updatable = this.updatables.get(tableName);
            if (updatable != null) {
              TableMBeanBridge tableMBeanBridge = updatable.getSelf();
              MemoryAnalyticsData memoryAnalytics = tableMBeanBridge.getMemoryAnalytics();
              if (memoryAnalytics != null) {
                String indexName = memoryAnalyticsVTI.getString(this.memAnaHolder.indexNameIndex);

                if (indexName == null) { // this row is for a TABLE
                  memoryAnalytics.updateMemoryAnalytics(Double.parseDouble(memoryAnalyticsVTI.getString(this.memAnaHolder.entrySizeIndex)),
                      Double.parseDouble(memoryAnalyticsVTI.getString(this.memAnaHolder.keySizeIndex)),
                      memoryAnalyticsVTI.getLong(this.memAnaHolder.numRowsIndex) );
                }
              }
              updatable.setUpdateToken(updateToken);
            } else {
              // TableMBean with the name not found, continue
              continue;
            }
          } catch (SQLException e) {
            logFine(e);
          }
          // TableMBean might have other updates to collect
          if (updatable != null) {
            // null check guards against SQLException for
            // memoryAnalyticsVTI.getString(this.maHolder.tableNameIndex);
            // We should still try to update the table MBean for other data
            updatable.update();
          }
        }
        this.memAnaHolder.lastUpdateTime = System.currentTimeMillis();
      } else { // Call update for non memory analytics data here
        Set<Entry<String, Updatable<TableMBeanBridge>>> entrySet = this.updatables.entrySet();
        for (Entry<String, Updatable<TableMBeanBridge>> entry : entrySet) {
          updatable = entry.getValue();
          if (updatable != null) {
            updatable.update();
          }
        }
      }
    } catch (SQLException e) {
      logFine(e);
    } catch (Exception e) {
      logFine(e);
    }finally {
      if (this.memAnaHolder != null) {
        this.memAnaHolder.reset();
      }
    }
    if (this.updateNumer == Integer.MAX_VALUE) {
      this.updateNumer = 0; // reset to zero
    }
    this.updateNumer++;
  }

  // NOTE: only for testing
  public long lastMemoryAnalyticsQueryTime() {
    if (this.memAnaHolder != null) {
      return this.memAnaHolder.lastUpdateTime;
    } else {
      return ManagementConstants.NOT_AVAILABLE_LONG;
    }
  }

  @Override
  public void cleanUp() {
    if (this.memAnaHolder != null) {
      this.memAnaHolder.memAnaVTI = null;
    }
    this.memAnaHolder = null;
    super.cleanUp();
  }

  /**
   *
   * @author Abhishek Chaudhari
   * @since gfxd 1.0
   */
  public static class MemoryAnalyticsHolder {
    // update from MemoryAnalytics every 600 seconds as it's resource intensive.
    public static final int DEFAULT_MEMANA_QUERY_INTERVAL_SEC = 600;
    public static final String MEMANA_QUERY_INTERVAL_PROPERTY = "tableAnalyticsUpdateIntervalSeconds";

    private MemoryAnalyticsVTI memAnaVTI;
    int tableNameIndex = ManagementConstants.NOT_AVAILABLE_INT;
    int entrySizeIndex = ManagementConstants.NOT_AVAILABLE_INT;
    int keySizeIndex   = ManagementConstants.NOT_AVAILABLE_INT;
    int numRowsIndex   = ManagementConstants.NOT_AVAILABLE_INT;
    int indexNameIndex = ManagementConstants.NOT_AVAILABLE_INT;
    int indexTypeIndex = ManagementConstants.NOT_AVAILABLE_INT;

    private int updatesToSkip;

    public long lastUpdateTime = ManagementConstants.NOT_AVAILABLE_LONG;

    MemoryAnalyticsHolder( MemoryAnalyticsVTI memAnaVTI) {
      this.memAnaVTI = memAnaVTI;

      int updateInterval = DEFAULT_MEMANA_QUERY_INTERVAL_SEC;
      try {
        updateInterval = SystemProperties.getServerInstance().getInteger(
            MEMANA_QUERY_INTERVAL_PROPERTY, DEFAULT_MEMANA_QUERY_INTERVAL_SEC);
      } catch (NumberFormatException e) {
        updateInterval = DEFAULT_MEMANA_QUERY_INTERVAL_SEC;
      }
      
      this.updatesToSkip = (updateInterval*1000)/MBeanUpdateScheduler.getConfiguredUpdateRate();
     }
    
    
    public void init() {
      try {
        if (this.tableNameIndex == -1) {
          ResultSetMetaData metaData = memAnaVTI.getMetaData();
          int columnCount = metaData.getColumnCount();
          for (int i = 1; i <= columnCount; i++) {
            if (metaData.getColumnName(i).equals(MemoryAnalyticsVTI.TABLE_NAME)) {
              this.tableNameIndex = i;
            } else if (metaData.getColumnName(i).equals(MemoryAnalyticsVTI.ENTRY_SIZE)) {
              this.entrySizeIndex = i;
            } else if (metaData.getColumnName(i).equals(MemoryAnalyticsVTI.KEY_SIZE)) {
              this.keySizeIndex = i;
            } else if (metaData.getColumnName(i).equals(MemoryAnalyticsVTI.NUM_ROWS)) {
              this.numRowsIndex = i;
            } else if (metaData.getColumnName(i).equals(MemoryAnalyticsVTI.INDEX_NAME)) {
              this.indexNameIndex = i;
            } else if (metaData.getColumnName(i).equals(MemoryAnalyticsVTI.INDEX_TYPE)) {
              this.indexTypeIndex = i;
            }
          }
        }
      } catch (SQLException e) {
        Misc.getCacheLogWriterNoThrow().fine(e);
      }
    }

    /**
     * NOTE: There's a built-in reset in MemoryAnalyticsVTI.next() now It's
     * returns false once but the next call to MemoryAnalyticsVTI.next() would
     * begin again. See following code:
     * <code>
     * if (!hasMoreContainers) {
     *   containerList = null; return false;
     * }</code>
     */
    void reset() {
      // NO-OP. Kept if there's an explicit reset in MemoryAnalyticsVTI in future
    }
  }
}

/**
 *
 * @author Abhishek Chaudhari
 * @since gfxd 1.0
 */
interface Updatable<T> {
  T getSelf();
  void update();
  void setUpdateToken(int updateToken);
}
