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
 * The shared map holds cache server records.  The keys are names given to the
 * cache servers by the application at they are started.  The values are
 * instances of {@link CacheServerHelper#Record}, containing relevant info.
 * <p>
 * Example: key "svr5" might map to "hostsrv:40404(Linux)".
 */

public class CacheServerBlackboard extends Blackboard {

  public static CacheServerBlackboard blackboard;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public CacheServerBlackboard() {
  }

  public CacheServerBlackboard(String name, String type) {
    super(name, type, CacheServerBlackboard.class);
  }

  /**
   * Creates a singleton blackboard.
   */
  public static synchronized CacheServerBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new CacheServerBlackboard("CacheServerBlackboard", "RMI");
    }
    return blackboard;
  }
}
