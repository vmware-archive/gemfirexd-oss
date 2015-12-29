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
package sql.backupAndRestore;

import java.util.List;

import hydra.Log;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;

/**
 * The BackupAndRestoreUtils class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 1.4
 */
public class BackupAndRestoreUtils {

  public static double CheckDataStoreBytesInUse() {
    double dataStoreBytesInUse = 0;
    String archiveName = BackupAndRestorePrms.getArchiveName();
    Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-archiveName=" + archiveName);
    String spec = "*" + archiveName + "* " // search all  Server archives
                  + "PartitionedRegionStats "
                  + "* " // match all instances
                  + "dataStoreBytesInUse "
                  + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                  + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                  + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List aList = PerfStatMgr.getInstance().readStatistics(spec);
    if (aList == null) {
      Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-No Stats Found for spec:" + spec);
    } else {
      Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-Getting Stats for spec:" + spec);
      for (Object anAList : aList) {
        PerfStatValue stat = (PerfStatValue) anAList;
        double statMax = stat.getMax();
        Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-statMax=" + statMax);
        dataStoreBytesInUse += statMax;
      }
      Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-dataStoreBytesInUse=" + dataStoreBytesInUse);
      Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-dataStoreBytesInUse(MB)=" + dataStoreBytesInUse / (1024*1024));
      Log.getLogWriter().info("BackupAndRestoreUtils.CheckDataStoreBytesInUse-dataStoreBytesInUse(GB)=" + dataStoreBytesInUse / (1024*1024*1024));
    }
    return dataStoreBytesInUse;
  }

  public static double GetMBInUse(){
    final double mbInUse = CheckDataStoreBytesInUse() / (1024 * 1024);
    Log.getLogWriter().info("BackupAndRestoreUtils.GetMBInUse-mbInUse=" + mbInUse);
    return mbInUse;
  }

}
