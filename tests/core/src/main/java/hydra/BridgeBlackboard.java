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
package hydra;

import hydra.blackboard.Blackboard;

/**
 * Holds a singleton instance per VM.
 * <p>
 * The shared map holds bridge server endpoints.  The keys are logical hydra
 * VM IDs, as returned by {@link RemoteTestModule#getMyVmid}.  The values are
 * instances of {@link BridgeHelper.Endpoint}, suitable for use by bridge
 * loaders and writers.
 * <p>
 * Example: key 7 might map to "vm_7_bridge2=hostsrv:40404".
 */

public class BridgeBlackboard extends Blackboard {

  public static BridgeBlackboard blackboard;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public BridgeBlackboard() {
  }

  public BridgeBlackboard(String name, String type) {
    super(name, type, BridgeBlackboard.class);
  }

  /**
   * Creates a singleton blackboard.
   */
  public static synchronized BridgeBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new BridgeBlackboard("BridgeBlackboard", "RMI");
    }
    return blackboard;
  }
}
