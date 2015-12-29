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

package cacheperf.gemfire.morgan;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.NanoTimer;
import hydra.*;
import java.util.Properties;

/**
 *  Client used to measure membership performance.
 */

public class MemberClient {

  /**
   *  TASK to bounce the connection to a distributed system and time it.
   */
  public static void bounceConnectionTask() {
    MemberClient c = new MemberClient();
    c.bounceConnection();
  }
  private void bounceConnection() {
    boolean fine = Log.getLogWriter().fineEnabled();

    // get the distributed system properties
    DistributedSystem ds = null;
    Properties p = getDistributedSystemProperties();
    if (fine) Log.getLogWriter().fine("Using connection properties " + p);

    // time connect and disconnect
    NanoTimer timer = new NanoTimer();
    long connectTime = 0L;
    long disconnectTime = 0L;
    int ops = 50;
    for (int i = 0; i < ops; i++) {

      // time a connect
      if (fine) Log.getLogWriter().fine("Connecting");
      timer.reset();
      ds = DistributedSystem.connect(p);
      connectTime += timer.reset();

      // time a disconnect
      if (fine) Log.getLogWriter().fine("Disconnecting");
      timer.reset();
      ds.disconnect();
      disconnectTime += timer.reset();
    }

    // write report
    String report = "Total operations: " + ops + "\n" +
      "Average connect time: " + averageMs(connectTime, ops) + " ms\n" +
      "Average disconnect time: " + averageMs(disconnectTime, ops) + " ms\n"
      ;
    Log.getLogWriter().info(report);
    FileUtil.writeToFile("perfreport.txt", report);
  }
  private double averageMs(long ns, long i) {
    return (double)ns/(double)(i*1000000);
  }
  private Properties getDistributedSystemProperties() {
    String gemfireName = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    GemFireDescription gfd =
       TestConfig.getInstance().getGemFireDescription(gemfireName);
    Properties p = gfd.getDistributedSystemProperties();
    // turn off the statarchiver since it is content-free
    p.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "");
    return p;
  }
}
