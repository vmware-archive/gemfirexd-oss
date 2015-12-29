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

package hydratest.security;

import hydra.BasePrms;
import hydra.Log;

/**
 * A class used to store keys for hydra test settings.
 */
public class SecurityTestPrms extends BasePrms {

  /**
   * boolean (default:false)
   * <p>
   * Whether the test should use a bogus password to fail authentication.
   */
  public static Long useBogusPassword;
  public static boolean useBogusPassword() {
    Long key = useBogusPassword;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * int (default:0:no expiration)
   * <p>
   * Number of seconds after which to expire default credentials.
   */
  public static Long expireSeconds;
  public static int getExpireSeconds() {
    Long key = expireSeconds;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  static {
    setValues(SecurityTestPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("securitytestprms", "info");
    dumpKeys();
  }
}
