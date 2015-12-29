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
package cacheperf.comparisons.snapshot;

import hydra.*;
import hydra.blackboard.Blackboard;

import cacheperf.*;
import perffmwk.*;
import util.*;

import java.io.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.snapshot.*;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.*;

/**
 * Extends CachePerfClient to include export and import commands plus basic validation
 */

public class SnapshotClient extends CachePerfClient {

  static final String snapshotFileBase = "snapshot";

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   *  TASK to export the region snapshot (RegionSnapshotService.save()).
   */
  public static synchronized void exportRegionSnapshot() {
    SnapshotClient c = new SnapshotClient();
    c.exportRegion();
  }

  protected void exportRegion() {
    long leader = SnapshotBlackboard.getInstance().getSharedCounters().incrementAndRead(SnapshotBlackboard.leader);
    if (leader == 1) {
      Region aRegion = CacheHelper.getCache().getRegion(RegionPrms.DEFAULT_REGION_NAME);
      RegionSnapshotService snapshot = aRegion.getSnapshotService();

      // Write this to the local disk (one directory up from the systemDir)
      GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
      String currDirName = (new File(gfd.getSystemDirectoryStr())).getParent();
      String snapshotFilename = currDirName + File.separator + snapshotFileBase;

      try {
        Log.getLogWriter().info("Exporting " + aRegion.getName() + " containing " + aRegion.size() + " entries");
        snapshot.save(new File(snapshotFilename), SnapshotFormat.GEMFIRE);
        Log.getLogWriter().info("Exported " + aRegion.getName() + " containing " + aRegion.size() + " entries");
        Blackboard bb = SnapshotBlackboard.getInstance();
        // clear leader flag, set regionSize and exportHost for import step
        bb.getSharedCounters().zero(SnapshotBlackboard.leader);
        bb.getSharedCounters().setIfLarger(SnapshotBlackboard.regionSize, aRegion.size());
        bb.getSharedMap().put(SnapshotBlackboard.exportHost, HostHelper.getLocalHost());
      } catch (IOException ioe) {
        throw new TestException("Caught " + ioe + " on export of " + aRegion.getFullPath());
      }
    }
  }

  /**
   *  TASK to import the region snapshot (RegionSnapshotService.load()).
   *  Import from the same host which exported (so we aren't reading the snapshot across the network).
   */
  public static synchronized void importRegionSnapshot() {
    SnapshotClient c = new SnapshotClient();
    c.importRegion();
  }

  protected void importRegion() {
    Blackboard bb = SnapshotBlackboard.getInstance();
    String exportHost = (String)bb.getSharedMap().get(SnapshotBlackboard.exportHost);

    if (exportHost.equalsIgnoreCase(HostHelper.getLocalHost())) {
      long leader = SnapshotBlackboard.getInstance().getSharedCounters().incrementAndRead(SnapshotBlackboard.leader);
      if (leader == 1) {
        Region aRegion = CacheHelper.getCache().getRegion(RegionPrms.DEFAULT_REGION_NAME);
        Log.getLogWriter().info("before import, region contains " + aRegion.size() + " entries");
        RegionSnapshotService snapshot = aRegion.getSnapshotService();
        // Write this to the local disk (one directory up from the systemDir)
        GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
        String currDirName = (new File(gfd.getSystemDirectoryStr())).getParent();
        String snapshotFilename = currDirName + File.separator + snapshotFileBase;
        Log.getLogWriter().info("Importing snapshot from " + snapshotFilename); 
        try {
          snapshot.load(new File(snapshotFilename), SnapshotFormat.GEMFIRE);
          Log.getLogWriter().info("After import " + aRegion.getName() + " contains " + aRegion.size() + " entries");
        } catch (Exception e) {    // IOException or ClassCastException
          throw new TestException("Caught " + e + " on import of " + aRegion.getFullPath());
        }
      }
    }
  }

  /**
   *  TASK to validate the number of entries in the region (after import)
   */
  public static synchronized void verifyRegionSnapshot() {
    SnapshotClient c = new SnapshotClient();
    c.verify();
  }

  protected void verify() {
    Region aRegion = CacheHelper.getCache().getRegion(RegionPrms.DEFAULT_REGION_NAME);
    long expectedRegionSize = SnapshotBlackboard.getInstance().getSharedCounters().read(SnapshotBlackboard.regionSize);
    int actualRegionSize = aRegion.size();
    if (actualRegionSize != expectedRegionSize) {
      throw new TestException("Region size (" + actualRegionSize + " is not equal to expectedRegionSize (" + expectedRegionSize);
    }
  }
}
