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
package hydra.gemfirexd;

import hydra.blackboard.Blackboard;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds a singleton instance per VM.
 * <p>
 * The shared map holds locator endpoints.  The keys are logical hydra
 * VM IDs, as returned by {@link RemoteTestModule#getMyVmid}.  The values are
 * instances of {@link LocatorHelper.Endpoint}.
 * <p>
 * Example: key 7 might map to "vm_7_ds2=hostsrv:40404".
 */
public class LocatorBlackboard extends Blackboard {

  public static LocatorBlackboard blackboard;
  private static Map<String,LocatorBlackboard> dsBlackboards = new HashMap();

  public static int locatorLock;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public LocatorBlackboard() {
  }

  public LocatorBlackboard(String name, String type) {
    super(name, type, LocatorBlackboard.class);
  }

  /**
   * Creates a singleton blackboard.
   */
  public static synchronized LocatorBlackboard getInstance() {
    if (blackboard == null) {
      blackboard =
        new LocatorBlackboard("LocatorBlackboard", "RMI");
    }
    return blackboard;
  }

  /**
   * Creates a singleton blackboard with the given distributed system name.
   */
  public static synchronized LocatorBlackboard getInstance(String dsName) {
    LocatorBlackboard dsBlackboard = dsBlackboards.get(dsName);
    if (dsBlackboard == null) {
      dsBlackboard = new LocatorBlackboard(dsName, "RMI");
    }
    return dsBlackboard;
  }
}
