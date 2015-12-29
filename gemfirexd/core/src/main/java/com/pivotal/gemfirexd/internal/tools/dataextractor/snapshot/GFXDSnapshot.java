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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.snapshot.GFSnapshot;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.tools.dataextractor.extractor.GemFireXDDataExtractorImpl;
import com.pivotal.gemfirexd.internal.tools.dataextractor.report.views.RegionViewInfoPerMember;

/****
 * 
 * @author bansods
 * @author jhuynh
 */
public final class GFXDSnapshot extends GFSnapshot {

  private static final String DDL_META_REGION_PATH = Region.SEPARATOR
      + GemFireStore.DDL_STMTS_REGION;
  public static final String QUEUE_REGION_PREFIX = "/AsyncEventQueue_GEMFIRE_HDFS_BUCKETSORTED_QUEUE";
  public static final String QUEUE_REGION_STRING = "AsyncEventQueue_GEMFIRE_HDFS_BUCKETSORTED_QUEUE";
  public static final String QUEUE_REGION_SHORT = "GF_QUEUE";
  public static final String PDX_TYPES_REGION = "/GFXD_PdxTypes";

  private GFXDSnapshot() {
    super(true);
  }

  /**
   * Creates a snapshot file and provides a serializer to write entries to the
   * snapshot.
   * 
   * @param snapshot
   *          the snapshot file
   * @param region
   *          the region name
   * @param regionViewInfo
   *          TODO
   * @return the callback to allow the invoker to provide the snapshot entries
   * @throws IOException
   *           error writing the snapshot file
   */
  public static SnapshotWriter create(File snapshot, String region,
      List<GFXDSnapshotExportStat> listOfStats,
      RegionViewInfoPerMember regionViewInfo, String stringDelimiter)
      throws IOException {

    try {
      final GFXDSnapshotExporter out = new GFXDSnapshotExporter(snapshot, region,
          listOfStats, regionViewInfo, stringDelimiter);
      //GemFireXDDataExtractorImpl.logInfo("#SB Extracting table : " + region);
      if (!out.isInitOk()) {
        return null;
      }
      return new SnapshotWriter() {

        public void snapshotEntry(SnapshotRecord entry) throws IOException {
          // no-op in xd case for now
        }

        public void snapshotEntry(Object key, Object value,
            VersionTag versionTag, long lastModified) throws IOException {
          out.writeSnapshotEntry(key, value, versionTag, lastModified);
        } 

        @Override
        public void snapshotEntry(SnapshotRecord entry,
            VersionTag versionTag, long lastModified) throws IOException {
          out.writeSnapshotEntry(entry, versionTag, lastModified);
        }

        @Override
        public void snapshotComplete() throws IOException {
          out.close();
        }
      };
    }
    catch(FileNotFoundException fnfe) {
        GemFireXDDataExtractorImpl.logSevere("Unable to export table due to " + fnfe, true);	
	return null;
    }
  }

  public static String[] getSchemaTableBucketNames(String regionName) {
    String schemaName = null, tableName = null, bucketName = null;
    String names[] = new String[3];
    if (regionName.equals(DDL_META_REGION_PATH)) {
      tableName = regionName;
    } else if (regionName.startsWith("/_")) {
      // Other type of meta region
      tableName = regionName;
    } else if (regionName.contains(QUEUE_REGION_SHORT)) {
    } else {
      String[] regionStrings = regionName.split("/");
      schemaName = regionStrings[1];
      tableName = regionStrings[2];
      if (regionStrings.length > 4) {
        bucketName = regionStrings[4];
        bucketName = bucketName.replace("/", "_");
      }
    }

    names[0] = schemaName;
    names[1] = tableName;
    names[2] = bucketName;
    return names;
  }
}
