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

/**
 * Holds ports for Derby network servers.
 */
public class NetworkServerBlackboard extends Blackboard {

  private static NetworkServerBlackboard blackboard;

  public static int connectionCount;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public NetworkServerBlackboard() {
  }

  /**
   * Creates a blackboard using the specified name and transport type.
   */
  public NetworkServerBlackboard(String name, String type) {
    super(name, type, NetworkServerBlackboard.class);
  }

  /**
   * Creates a blackboard.
   */
  public static synchronized NetworkServerBlackboard getInstance() {
    if (blackboard == null) {
      blackboard =
        new NetworkServerBlackboard("NetworkServerBlackboard", "rmi");
    }
    return blackboard;
  }
}
