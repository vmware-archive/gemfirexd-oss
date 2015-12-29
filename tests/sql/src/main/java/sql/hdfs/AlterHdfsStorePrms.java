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
package sql.hdfs;

import hydra.BasePrms;
import hydra.Log;
import util.TestException;
import sql.hdfs.AlterHDFSTest.AlterType;

public class AlterHdfsStorePrms extends BasePrms {

  static final String RW_BUCKET_ORGANIZER = "RW_BUCKET_ORGANIZER";
  static final String WO_BUCKET_ORGANIZER = "WO_BUCKET_ORGANIZER";
  static final String HDFS_AEQ = "HDFS_AEQ";
  static final String COMPACTION_MANAGER_DISABLE_MINOR = "COMPACTION_MANAGER_DISABLE_MINOR";
  static final String COMPACTION_MANAGER_DISABLE_MAJOR = "COMPACTION_MANAGER_DISABLE_MAJOR";
  static final String COMPACTION_MANAGER = "COMPACTION_MANAGER";
 
  public static Long alterTypeString;
  public static AlterType getAlterType() {
    Long key = alterTypeString;
    String val = tab().stringAt(AlterHdfsStorePrms.alterTypeString, null);
    AlterType alterType;
 
    Log.getLogWriter().info("Test configured with alterType = " + val);
    if (val.equalsIgnoreCase(RW_BUCKET_ORGANIZER)) {
      alterType = AlterType.RW_BUCKET_ORGANIZER;
    } else if (val.equalsIgnoreCase(WO_BUCKET_ORGANIZER)) {
      alterType = AlterType.WO_BUCKET_ORGANIZER;
    } else if (val.equalsIgnoreCase(HDFS_AEQ)) {
      alterType = AlterType.HDFS_AEQ;
    } else if (val.equalsIgnoreCase(COMPACTION_MANAGER_DISABLE_MINOR)) {
      alterType = AlterType.COMPACTION_MANAGER_DISABLE_MINOR;
    } else if (val.equalsIgnoreCase(COMPACTION_MANAGER_DISABLE_MAJOR)) {
      alterType = AlterType.COMPACTION_MANAGER_DISABLE_MAJOR;
    } else if (val.equalsIgnoreCase(COMPACTION_MANAGER)) {
      alterType = AlterType.COMPACTION_MANAGER;
    } else {
        throw new TestException("Unknown AlterHdfsStorePrms.alterType " + val);
    }
    return alterType;
  }

  /** (int) The number of seconds to run a test (currently used for concParRegHA)
   *  to terminate the test. We cannot use hydra's taskTimeSec parameter
   *  because of a small window of opportunity for the test to hang due
   *  to the test's "concurrent round robin" type of strategy.
   */ 
  public static Long secondsToRun;
  public static int getSecondsToRun() {
    Long key = secondsToRun;
    int value = tab().intAt(key, 300);
    return value;
  }

  /** (boolean) Whether or not to launch a conflicting procedure while altering the hdfs store
   *  AlterType.HDFS_AEQ => run sys.HDFS_FLUSH_QUEUE
   *  AlterType.RW_BUCKET_ORGANIZER =>  run sys.HDFS_FORCE_COMPACTION
   *  AlterType.COMPACTION_MANAGER => run sys.HDFS_FORCE_COMPACTION
   *  AlterType.WO_BUCKET_ORGANIZER => run sys.HDFS_FORCE_WRITEONLY_FILEROLLOVER
   */
  public static Long launchProcedure;
  public static boolean getLaunchProcedure() {
    Long key = launchProcedure;
    boolean value = tab().booleanAt(key, false);
    return value;
  }

  static {
     BasePrms.setValues(AlterHdfsStorePrms.class);
  }  
}

 
 

