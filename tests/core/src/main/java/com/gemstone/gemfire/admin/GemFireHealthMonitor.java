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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.admin.internal.DistributedSystemConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.admin.OperationCancelledException;
import java.io.*;
import java.util.*;

/**
 * A program that continually monitors the health of GemFire.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class GemFireHealthMonitor {
  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

//  /**
//   * Prints usage information about this program
//   */
//  private static void usage(String s) {
//    err.println("\n** " + s + "\n");
//    err.println("usage: java GemFireHealthmonitor [gemfire.properties]");
//
//    err.println("");
//    System.exit(1);
//  }

  public static void main(String[] args) throws Throwable {
    Properties props = new Properties();
    if (args.length > 0) {
      props.load(new FileInputStream(args[0]));
    }

    DistributionConfigImpl dc = new DistributionConfigImpl(props);
    DistributedSystemConfig dsc =
      new DistributedSystemConfigImpl(dc, DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
    AdminDistributedSystemImpl system = (AdminDistributedSystemImpl)
      AdminDistributedSystemFactory.getDistributedSystem(dsc);

    system.connect(new LocalLogWriter(LogWriterImpl.ALL_LEVEL,
                                      System.out));

    GemFireHealth health = system.getGemFireHealth();
    GemFireHealthConfig config =
      health.getDefaultGemFireHealthConfig();
    config.setMaxLoadTime(100);
    config.setHealthEvaluationInterval(1);
    health.setDefaultGemFireHealthConfig(config);

    DistributedSystemHealthConfig dsHealthConfig =
      health.getDistributedSystemHealthConfig();
    dsHealthConfig.setMaxDepartedApplications(0L);
    health.setDistributedSystemHealthConfig(dsHealthConfig);

    while (true) {
      try {
        char c;
        GemFireHealth.Health code = health.getHealth();
        if (code == GemFireHealth.GOOD_HEALTH) {
          c = 'G';

        } else if (code == GemFireHealth.OKAY_HEALTH) {
          c = 'O';
          System.out.println(health.getDiagnosis());

        } else if (code == GemFireHealth.POOR_HEALTH) {
          c = 'P';
          System.out.println(health.getDiagnosis());

        } else {
          String s = "Unknown code: " + code;
          throw new IllegalStateException(s);
        }

        out.print(c);
        out.flush();
        Thread.sleep(500);

      } catch (OperationCancelledException ex) {
        ex.printStackTrace(System.out);
      }
    }
  }

}
