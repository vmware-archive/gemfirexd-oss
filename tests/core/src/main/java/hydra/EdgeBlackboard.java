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
 * lists of {@link BridgeHelper.Endpoint}, suitable for use by bridge loaders
 * and writers.
 * <p>
 * Example: key 7 might map to a list containing "vm_7_bridge2=hostsrv:40404".
 */

public class EdgeBlackboard extends Blackboard {

  public static EdgeBlackboard blackboard;

  /**
   * Counter used to provide a unique, round-robin, starting index into the
   * endpoint list.
   */
  public static int startIndex;

  /**
   * Counters used to provide a unique, round-robin, starting index into the
   * endpoint list for each wan site.  Up to 6 wan sites are supported.
   */
  public static int startIndex1;
  public static int startIndex2;
  public static int startIndex3;
  public static int startIndex4;
  public static int startIndex5;
  public static int startIndex6;
  public static int startIndex7;
  public static int startIndex8;
  public static int startIndex9;
  public static int startIndex10;
  public static int startIndex11;
  public static int startIndex12;
  public static int startIndex13;
  public static int startIndex14;
  public static int startIndex15;
  public static int startIndex16;
  public static int startIndex17;
  public static int startIndex18;
  public static int startIndex19;
  public static int startIndex20;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public EdgeBlackboard() {
  }

  public EdgeBlackboard(String name, String type) {
    super(name, type, EdgeBlackboard.class);
  }

  /**
   * Creates a singleton blackboard.
   */
  public static synchronized EdgeBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new EdgeBlackboard("EdgeBlackboard", "RMI");
    }
    return blackboard;
  }
}
