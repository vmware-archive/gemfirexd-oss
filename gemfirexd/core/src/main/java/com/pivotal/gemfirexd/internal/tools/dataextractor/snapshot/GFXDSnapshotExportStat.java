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
package com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView;
import com.pivotal.gemfirexd.internal.tools.dataextractor.report.views.RegionViewInfoPerMember;

public class GFXDSnapshotExportStat {
  private String serverName; // set in salvage tool after disk store is
                             // recovered, used to simplify summary report
  private String schemaName;
  private String tableName;
  private String bucketName;
  private String tableType;
  private String fileName;
  private List entryDecodeErrors = new ArrayList();
  private boolean completedFile = true;
  int numValuesDecoded = 0;
  int numValuesNotDecoded = 0;
  private long maxSeqId;
  private int numDdlStmts = 0;
  private long lastModifiedTime = 0;
  private RegionViewInfoPerMember regionViewInfo;
  private boolean corrupt = false;
  private boolean isQueueExport = false;

  public GFXDSnapshotExportStat(DiskRegionView drv) {
    String regionName = drv.getName();
    String names[] = GFXDSnapshot.getSchemaTableBucketNames(regionName);
    this.schemaName = names[0];
    this.tableName = names[1];
    this.bucketName = names[2];
    this.numValuesDecoded = drv.getRecoveredEntryCount();
  }

  public GFXDSnapshotExportStat(String schemaName, String tableName,
      String bucketName, RegionViewInfoPerMember regionViewInfo) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.bucketName = bucketName;
    this.regionViewInfo = regionViewInfo;
  }

  public String getFileName() {
    return fileName;
  }

  // Set after renaming of the file has taken place
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public List getEntryDecodeErrors() {
    return entryDecodeErrors;
  }

  public void addEntryDecodeError(String error) {
    entryDecodeErrors.add(error);
  }

  public boolean isCompletedFile() {
    return completedFile;
  }

  public void setCompletedFile(boolean completedFile) {
    this.completedFile = completedFile;
  }

  public int getNumValuesDecoded() {
    return numValuesDecoded;
  }

  public void incrementNumValuesDecoded() {
    this.numValuesDecoded++;
  }

  // Used by tests
  public void setNumValuesDecoded(int numValuesDecoded) {
    this.numValuesDecoded = numValuesDecoded;
  }

  public int getNumValuesNotDecoded() {
    return numValuesNotDecoded;
  }

  public void incrementNumValuesNotDecoded() {
    this.numValuesNotDecoded++;
  }

  public String getSchemaTableName() {
    if (bucketName == null) {
      return schemaName + "_" + tableName;
    }
    return schemaName + "_" + tableName + "_" + bucketName;
  }

  public long getMaxSeqId() {
    return maxSeqId;
  }

  public void setMaxSeqId(long maxSeqId) {
    this.maxSeqId = maxSeqId;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public String getServerName() {
    return this.serverName;
  }

  public int getNumDdlStmts() {
    return numDdlStmts;
  }

  public void setNumDdlStmts(int numOfDdlStmts) {
    this.numDdlStmts = numOfDdlStmts;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public void setLastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

  public String getTableType() {
    return tableType;
  }

  public RegionViewInfoPerMember getRegionViewInfo() {
    return this.regionViewInfo;
  }

  public boolean isCorrupt() {
    return corrupt;
  }

  public void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  public boolean isSameTableStat(GFXDSnapshotExportStat other) {
    return this.getSchemaTableName().equals(other.getSchemaTableName());
  }
  
  public boolean isQueueExport() {
    return this.isQueueExport;
  }
  
  public void setQueueExport(boolean isQueueExport) {
    this.isQueueExport = isQueueExport;
  }
}