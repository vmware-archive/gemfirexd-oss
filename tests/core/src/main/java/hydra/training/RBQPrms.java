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
package hydra.training;

import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.TestConfig;

/**
 * Hydra configuration parameters for {@link RemoteBlockingQueue}
 * tests.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class RBQPrms extends BasePrms {

  static {
    // Set the values of the Long fields
    BasePrms.setValues(RBQPrms.class);
  }

  /** The maximum number of elements allowed in the queue
   *
   * @see #getQueueCapacity */
  public static Long queueCapacity;

  /** Are we debugging a run of this test?
   *
   * @see #debug() */
  public static Long debug;

  /** Does this test use the {@link RBQBlackboard}? 
   *
   * @see #useBlackboard() */
  public static Long useBlackboard;

  /////////////////////// Static Methods  ///////////////////////

  /**
   * Returns the maximum number of elements allowed in the queue.  The
   * default is 100.
   */
  public static int getQueueCapacity() {
    ConfigHashtable tab = TestConfig.tab();
    return tab.intAt(queueCapacity, 100);
  }

  /**
   * Returns whether or not we are debugging this run of the test.  If
   * <code>true</code>, then extra information will be logged by the
   * test.  The default value is <code>false</code>.
   */
  public static boolean debug() {
    ConfigHashtable tab = TestConfig.tab();
    return tab.booleanAt(debug, false);
  }

  /**
   * Returns whether or not this test uses the {@link RBQBlackboard}
   * to gather statistics about the test.  The default value is
   * <code>false</code>.
   */
  public static boolean useBlackboard() {
    ConfigHashtable tab = TestConfig.tab();
    return tab.booleanAt(useBlackboard, false);
  }

}
