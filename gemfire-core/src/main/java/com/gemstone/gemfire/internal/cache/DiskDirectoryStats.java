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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.NanoTimer;
//import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.*;

/**
 * GemFire statistics about Disk Directories
 *
 * @author Darrel Schneider
 *
 * @since 3.2
 */
public class DiskDirectoryStats {

  private static final StatisticsType type;

  ////////////////////  Statistic "Id" Fields  ////////////////////

  private static final int diskSpaceId;
  private static final int maxSpaceId;

  static {
    String statName = "DiskDirStatistics";
    String statDescription =
      "Statistics about a single disk directory for a region";

    final String diskSpaceDesc =
      "The total number of bytes currently being used on disk in this directory for oplog files.";
    final String maxSpaceDesc =
      "The configured maximum number of bytes allowed in this directory for oplog files. Note that some product configurations allow this maximum to be exceeded.";
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription,
       new StatisticDescriptor[] {
         f.createLongGauge("diskSpace", diskSpaceDesc, "bytes"),
         f.createLongGauge("maximumSpace", maxSpaceDesc, "bytes"),
       });

    // Initialize id fields
    diskSpaceId = type.nameToId("diskSpace");
    maxSpaceId = type.nameToId("maximumSpace");
  }

  //////////////////////  Instance Fields  //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>DiskRegionStatistics</code> for the given
   * region. 
   */
  public DiskDirectoryStats(StatisticsFactory f, String name) {
    this.stats = f.createStatistics(type, name);
  }

  /////////////////////  Instance Methods  /////////////////////

  public void close() {
    this.stats.close();
  }

  /**
   * Returns the current value of the "diskSpace" stat.
   */
  public long getDiskSpace() {
    return this.stats.getLong(diskSpaceId);
  }

  public void incDiskSpace(long delta) {
    this.stats.incLong(diskSpaceId, delta);
  }
  public void setMaxSpace(long v) {
    this.stats.setLong(maxSpaceId, v);
  }
  
  public Statistics getStats(){
    return stats;
  }
}
