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
 * Holds a singleton instance per JVM.
 * <p>
 * The shared map holds JMX manager endpoints. The keys are logical hydra
 * JVM IDs, as returned by {@link RemoteTestModule#getMyVmid}. The values are
 * instances of {@link JMXManagerHelper.Endpoint}.
 */
public class JMXManagerBlackboard extends Blackboard {

  public static JMXManagerBlackboard blackboard;

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public JMXManagerBlackboard() {
  }

  public JMXManagerBlackboard(String name, String type) {
    super(name, type, JMXManagerBlackboard.class);
  }

  /**
   * Creates a singleton blackboard.
   */
  public static synchronized JMXManagerBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new JMXManagerBlackboard("JMXManagerBlackboard", "RMI");
    }
    return blackboard;
  }
}
