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
package com.gemstone.gemfire;

import com.gemstone.gemfire.distributed.DistributedSystem;
import java.util.*;

/**
 * Tests the functionality of JOM {@link Statistics}.
 */
public class LocalStatisticsTest extends StatisticsTestCase {
    public LocalStatisticsTest(String name) {
      super(name);
    }

    /**
     * Returns a distributed system configured to not use shared
     * memory.
     */
    protected DistributedSystem getSystem() {
      if (this.system == null) {
        Properties props = new Properties();
        props.setProperty("statistic-sampling-enabled", "true");
        props.setProperty("statistic-archive-file", "StatisticsTestCase-localTest.gfs");
//         int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
//         props.setProperty("mcast-port", String.valueOf(unusedPort));
        // make it a loner since test does not distribution
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        this.system = DistributedSystem.connect(props);
      }

      return this.system;
    }
}
