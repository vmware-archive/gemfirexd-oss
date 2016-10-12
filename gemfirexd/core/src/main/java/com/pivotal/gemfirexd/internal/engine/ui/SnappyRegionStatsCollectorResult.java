/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.engine.ui;

import java.io.Serializable;

import com.gemstone.gemfire.cache.DataPolicy;

public class SnappyRegionStatsCollectorResult implements Serializable {
  private boolean isColumnTable = false;
  DataPolicy dataPolicy;
  private String regionName;
  private long rowCount = 0;
  private long sizeInMemory = 0;
  private long totalSize = 0;

  public SnappyRegionStatsCollectorResult(String regionName, DataPolicy datapolicy) {
    this.regionName = regionName;
    this.dataPolicy = datapolicy;
  }


  public Boolean isReplicatedTable() {
    if (this.dataPolicy == DataPolicy.PERSISTENT_REPLICATE || this.dataPolicy == DataPolicy.REPLICATE)
      return true;
    else
      return false;
  }

  public void setTotalSize(long totalSize) {
    this.totalSize = totalSize;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public boolean isColumnTable() {
    return isColumnTable;
  }

  public void setColumnTable(boolean columnTable) {
    isColumnTable = columnTable;
  }

  public DataPolicy getDataPolicy() {
    return dataPolicy;
  }

  public void setDataPolicy(DataPolicy dataPolicy) {
    this.dataPolicy = dataPolicy;
  }

  public long getRowCount() {
    return rowCount;
  }

  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  public long getSizeInMemory() {
    return sizeInMemory;
  }

  public void setSizeInMemory(long sizeInMemory) {
    this.sizeInMemory = sizeInMemory;
  }

  public long getTotalSize() {
    return this.totalSize;
  }

  public SnappyRegionStatsCollectorResult getCombinedStats(SnappyRegionStatsCollectorResult stats) {
    String regionName = this.isColumnTable ? stats.regionName : this.regionName;
    SnappyRegionStatsCollectorResult combinedStats = new SnappyRegionStatsCollectorResult(regionName, this.dataPolicy);

    if (this.isReplicatedTable()) {
      combinedStats.setRowCount(stats.rowCount);
      combinedStats.setTotalSize(stats.totalSize);
    } else {
      combinedStats.setRowCount(stats.rowCount + this.rowCount);
      combinedStats.setTotalSize(stats.totalSize + this.totalSize);
    }

    combinedStats.setSizeInMemory(stats.sizeInMemory + this.sizeInMemory);
    combinedStats.setColumnTable(this.isColumnTable ? this.isColumnTable : stats.isColumnTable);
    return combinedStats;
  }

}
