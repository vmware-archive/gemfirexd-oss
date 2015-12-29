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
import hydra.TestConfig;

/**
 * Configuration parameters for the {@link Server} test.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class ServerPrms extends BasePrms {

  /** (boolean) Are we debugging this test? */
  public static Long debug;

  /** (int) The port on which the server listens */
  public static Long port;

  /** (int) The port on which the failover server listens */
  public static Long failoverPort;

  //////////////////////  Static Methods  //////////////////////

  static {
    BasePrms.setValues(ServerPrms.class);
  }

  /**
   * Returns whether or not we are debugging this test.  By default,
   * this method returns <code>false</code>.  Most likely, this
   * parameter will be set to <code>true</code> in a
   * <code>local.conf</code>.
   */
  public static boolean debug() {
    return TestConfig.tab().booleanAt(debug, false);
  }

  /**
   * Returns the port on which the server should listen.  This
   * parameter has no default value.
   *
   * @throws HydraConfigException
   *         If no port has been specified
   */
  public static int getPort() {
    return TestConfig.tab().intAt(port);
  }

  /**
   * Returns the port on which the failover server should listen.
   * This parameter has no default value.
   *
   * @throws HydraConfigException
   *         If no port has been specified
   */
  public static int getFailoverPort() {
    return TestConfig.tab().intAt(failoverPort);
  }

}

