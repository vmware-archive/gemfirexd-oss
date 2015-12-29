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

import hydra.Log;
import java.util.List;

public class HDFSSqlTestVersionHelper {

  // starting with 1.4 we will run the sys.HDFS_FORCE_WRITEONLY_FILEROLLOVER procedure vs. sleeping
  public static void waitForWriteOnlyFileRollover(List<String> tables) {
    Log.getLogWriter().info("Executing system procedure to force WriteOnlyFileRollover ...");
    try {
      sql.SQLTest.forceWriteOnlyFileRollover(tables);
    } catch (Exception e) {
      Log.getLogWriter().info("Caught " +  e.getMessage() + " while executing HDFS_FORCE_WRITEONLY_FILEROLLOVER");
    }
  }
  
  public static StringBuffer appendBatchInterval(StringBuffer config, int batchTimeInterval) {
    config.append(" BATCHTIMEINTERVAL ").append(batchTimeInterval).append(" milliseconds");
    return config;
  }

  public static StringBuffer appendMajorCompactionInterval(StringBuffer config, int majorCompactionInterval) {
    config.append(" MAJORCOMPACTIONINTERVAL ").append(majorCompactionInterval).append(" minutes");
    return config;
  }
  
  public static StringBuffer appendWriteOnlyFileRolloverInterval(StringBuffer config, int writeOnlyFileRolloverInterval) {
    config.append(" WRITEONLYFILEROLLOVERINTERVAL ").append(writeOnlyFileRolloverInterval).append(" minutes" );
    return config;
  }
  
  public static StringBuffer appendPurgeInterval(StringBuffer config, int purgeInterval) {
    config.append(" PURGEINTERVAL ").append(purgeInterval).append(" minutes" );
    return config;
  }
}
