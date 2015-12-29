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
package com.pivotal.pxf.plugins.gemfirexd;

import java.io.IOException;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.MapRedRowRecordReader;
import com.pivotal.gemfirexd.hadoop.mapred.Row;
import com.pivotal.gemfirexd.hadoop.mapred.RowInputFormat;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.gemfirexd.util.GemFireXDManager;

/**
 * This implementation is currently taken from HdfsSplittableDataAccessor with
 * minor changes related to InputFormat and RecordReader implementations.
 * </p>
 * 
 * Explain why this doesn't extend from HdfsSplittableDataAccessor.
 * 1. We need to use GemFireXDManager
 * 2. EventInputFormat does not extend from FileInputFormat.
 */
public class GemFireXDAccessor extends Plugin implements ReadAccessor {

  private InputFormat<Key, Row> inputFormat = null;
  private GemFireXDManager gfxdManager;
  private InputSplit split;
  protected Configuration conf = null;
  protected MapRedRowRecordReader reader = null;
  protected ListIterator<InputSplit> iter = null;
  protected JobConf jobConf = null;
  protected Key key;
  protected Row data;

  private static Object lockObject = new Object();
  /**
   * Always accessed after synchronizing on lockObject.
   */
  private static String currentHomeDirs = null;
  /**
   * Always accessed after synchronizing on lockObject.
   * Restart of loner GemFireXD system waits until this value becomes zero.
   * @see #referenceCounts
   */
  private static int lonerRefCount = 0;

  /**
   * Map of schema.table name as key and an Object as value.
   * <P/>
   * If the value is an Integer, it indicates the number of split requests
   * currently active on that table (i.e. reference count for that table).
   * <P/>
   * If the value is a Long, it indicates that some split request on this table
   * encountered an exception at a time-stamp indicated by the Long value and
   * that that request has reset the reference count for that table before
   * returning so that other requests needing to restart loner system are not
   * blocked indefinitely.
   * <P/>
   * {@link #lonerRefCount} is essentially a sum of reference counts of all
   * individual tables being currently accessed.
   * 
   * @see #resetRefCount(String)
   * @see #incrementRefCount()
   * @see #decrementRefCount()
   */
  private static Map<String, Object> referenceCounts = new HashMap<String, Object>();

  /**
   * Used to pass on the schema.table name from accessor to resolver.
   */
  public static ThreadLocal<String> tableName = new ThreadLocal<String>(); 

  private boolean accessorClosed = true;

  private boolean isWriteOnly = false;

  private String[] pkColumns = null;

  private boolean newHomeDirFound = false;

  private static final int THRESHOLD_MILIS = 30000;

  /**
   * @param input
   * @throws IOException
   */
  public GemFireXDAccessor(InputData input) throws IOException {
    this(input, new GemFireXDManager(input));
  }

  /**
   * @param input
   * @param mgr
   * @throws IOException
   */
  public GemFireXDAccessor(InputData input, GemFireXDManager mgr) throws IOException {
    super(input);
    this.gfxdManager = mgr;
    //this.gfxdManager.startLonerGfxd();
    this.gfxdManager.readUserData();
    tableName.set(this.gfxdManager.getTable());
    this.split = this.gfxdManager.getSplit();

    this.inputFormat = this.gfxdManager.getInputFormat();
    this.conf = new Configuration();
    this.jobConf = new JobConf(this.conf, GemFireXDAccessor.class);
  }

  @Override
  public boolean openForRead() throws Exception {

    // Restart the running loner if it is not configured with the new home-dir.
    synchronized (lockObject) {
      try {
      if (needToShutdownLoner()) {
        shutdownLoner();
      }

      if (this.gfxdManager.getLogger().isDebugEnabled()) {
        this.gfxdManager.getLogger().debug(
            "Accessor getting reader for split " + this.split.toString());
      }
      this.gfxdManager.configureJob(this.jobConf, currentHomeDirs);
      this.reader = (MapRedRowRecordReader)inputFormat.getRecordReader(
          this.split, this.jobConf, null);

      // If the table does not have primary key(s) defined, throw exception if
      // A) checkpoint is false or B) the table is write-only.
      updateTableInfo();
      } catch (Exception e) {
        /*
         * Fix for #50089.
         * 
         * One side effect of this change is that if a new PXF query needs to
         * restart the loner system, then any currently running queries may fail
         * due to restart. But this is better than not being able to restart the
         * loner due to the count never being decremented to zero because
         * closeForRead() is not guaranteed to be called always.
         */
        resetLonerRefCount();
        throw e;
      }
      boolean checkpoint = this.jobConf.getBoolean(RowInputFormat.CHECKPOINT_MODE, true);
      if (!tableHasPK() && (!checkpoint || isWriteOnlyTable())) {
        throw new IllegalArgumentException("Table "
            + this.gfxdManager.getTable()
            + " does not have primary key(s) defined in GemFireXD."
            + " Querying event data from tables without primary key(s) is not supported.");
      }
      this.accessorClosed = false;
      incrementRefCount();
    }

    this.key = this.reader.createKey();
    this.data = this.reader.createValue();
    return true;
  }

  private boolean needToShutdownLoner() throws IOException {
    if (currentHomeDirs == null || currentHomeDirs.isEmpty()) {
      // No currentHomeDirs means loner is not up yet and hence no need to shut
      // it down.
      currentHomeDirs = this.gfxdManager.getHomeDir();
      return false;
    } else if (!isHomeDirKnown() || this.gfxdManager.isDDLTimeStampChanged()) {
      return true;
    }
    return false;
  }

  private boolean isHomeDirKnown() {
    StringTokenizer st = new StringTokenizer(currentHomeDirs, ",");
    while (st.hasMoreTokens()) {
      if (this.gfxdManager.getHomeDir().equalsIgnoreCase(st.nextToken())) {
        return true;
      }
    }
    this.newHomeDirFound = true;
    return false;
  }

  /**
   * Waits until either there are no active read requests or 30 seconds elapse.
   * Shuts down loner only in the former case while throws exception for latter.
   * 
   * This is always accessed under the lock on {@link #lockObject}
   * 
   * @throws IOException
   */
  private void shutdownLoner() throws IOException {
    long start = System.currentTimeMillis();

    while (lonerRefCount > 0) {
      try {
        this.gfxdManager.getLogger().info(
            "Waiting for existing read requests on data in hdfs-store(s) "
                + currentHomeDirs + " to complete. Active requests: "
                + lonerRefCount);
        lockObject.wait(5000);
      } catch (InterruptedException ie) {
        this.gfxdManager.getLogger().error("Returning early from accessor. " + ie);
        throw new IOException("Could not restart loner instance.");
      }
      if ((System.currentTimeMillis() - start) > 30000) {
        this.gfxdManager.getLogger().info(
            "Currently, " + lonerRefCount
                + " read requests are active on this node.");
        if (lonerRefCount > 0) {
          throw new IOException(
              "Read requests on tables in hdfs-store(s) "
                  + currentHomeDirs
                  + " are active currently. New read requests on tables in hdfs-store "
                  + this.gfxdManager.getHomeDir()
                  + " can be processed after these are completed.");
          // TODO Check if we are making any progress. Shutdown loner if we did
          // but the count is not changing and we have waited for a very long time.
        }
      }
    } // while

    // Shutdown, so that getRecordReader() starts a new instance.
    try {
      this.gfxdManager.shutdown();
      this.gfxdManager.getLogger().info("Shutdown of loner system with home-dir(s) " 
          + currentHomeDirs + " done.");

      if (currentHomeDirs == null || currentHomeDirs.isEmpty()) {
        currentHomeDirs = this.gfxdManager.getHomeDir();
      } else if (this.newHomeDirFound) {
        currentHomeDirs = new StringBuilder(currentHomeDirs).append(",")
            .append(this.gfxdManager.getHomeDir()).toString();
      }
    } catch (Exception e) {
      this.gfxdManager.getLogger().warn(
          "Shutdown of loner system with home-dir(s) " + currentHomeDirs
              + " failed. " + e);
    }
  }

  private void updateTableInfo()  throws IOException {
    this.gfxdManager.updateDDLTimeStampIfNeeded();
    Region region = Misc.getRegionForTable(this.gfxdManager.getTable(), false);
    if (region != null && region instanceof LocalRegion) {
      this.isWriteOnly = !((LocalRegion) region).isHDFSReadWriteRegion();
      this.pkColumns = ((GemFireContainer) ((LocalRegion) region)
          .getUserAttribute()).getExtraTableInfo().getPrimaryKeyColumnNames();
    } else {
      // Very unlikely to reach this case.
      this.gfxdManager.getLogger().warn(
          "Table " + this.gfxdManager.getTable() + " could not be identified.");
    }
  }

  private boolean tableHasPK() {
    return (this.pkColumns != null && this.pkColumns.length > 0);
  }

  private boolean isWriteOnlyTable() {
    return this.isWriteOnly;
  }

  /**
   * @throws IOException
   */
  @Override
  public void closeForRead() throws IOException {
    synchronized (lockObject) {
      if (this.reader != null) {
        this.reader.close();
      }
      decrementRefCount();
      this.accessorClosed = true;
      lockObject.notifyAll();
    }
  }

  public void finalize() {
    synchronized (lockObject) {
      if (!this.accessorClosed) {
        // close() was not called, so decrement the count here.
        decrementRefCount();
        lockObject.notifyAll();
      }
    }
  }

  private void incrementRefCount() throws IOException {
    String table = this.gfxdManager.getTable();
    Object value = referenceCounts.get(table);
    if (value != null && value instanceof Long) {
      Long currentTime = System.currentTimeMillis();
      if ((currentTime - (Long) value) <= THRESHOLD_MILIS) {
        referenceCounts.put(table, Long.valueOf(currentTime));
        throw new IOException(
            "Encountered failure while processing request on "
                + table + ". Please wait for "
                + (THRESHOLD_MILIS / 1000)
                + " seconds before firing any query on the same table.");
      }
      referenceCounts.put(table, Integer.valueOf(1));
    } else {
      Integer count = value == null ? Integer.valueOf(0) : (Integer)value;
      count = count + 1;
      referenceCounts.put(table, count);
    }
    ++lonerRefCount;
  }

  private void decrementRefCount() {
    Object value = referenceCounts.get(this.gfxdManager.getTable());
    if (value == null || value instanceof Long) { 
      return;
    }
    Integer count = (Integer) value;
    --lonerRefCount;
    count = count - 1;
    referenceCounts.put(this.gfxdManager.getTable(), count);
    if (lonerRefCount < 0) {
      lonerRefCount = 0;
    }
  }

  private static void resetRefCount(String table) {
    referenceCounts.put(table, Long.valueOf(System.currentTimeMillis()));

    // Do some cleanup
    int totalCount = 0;
    for (String tName : referenceCounts.keySet()) {
      Object value = referenceCounts.get(tName);
      if (value == null || value instanceof Long) {
        continue;
      }
      Integer cnt = (Integer)value;
      if (cnt >= 0) {
        totalCount += cnt;
      } else {
        referenceCounts.put(tName, 0);
      }
    }
    lonerRefCount = totalCount;
  }

  @Override
  public OneRow readNextObject() throws IOException {
    if (this.reader.next(this.key, this.data)) {
      return new OneRow(this.key, this.data);
    }
    return null;
  }

  /**
   * This is called in case of failure in resolver while parsing the field data.
   */
  public void resetLonerRefCount() {
    resetLonerRefCount(this.gfxdManager.getTable());
  }

  /**
   * This is called only in case of failure.
   */
  public static void resetLonerRefCount(String table) {
    synchronized (lockObject) {
      resetRefCount(table);
      lockObject.notifyAll();
    }
  }

}

