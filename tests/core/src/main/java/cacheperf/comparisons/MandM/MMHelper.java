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

package cacheperf.comparisons.MandM;

import management.test.cli.CommandTestVersionHelper;
import hydra.BridgeHelper;
import hydra.CacheHelper;

/**
 * @author lynn
 *
 */
public class MMHelper {
  
  public static MMHelper testInstance = null;

  //=================================================
  // initialization methods

  /** Creates and initializes a server or peer.
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new MMHelper();
      CacheHelper.createCache("cache1");
      CommandTestVersionHelper.createRegions();
    }
  }
  
  /** Log whether this member is a manager or not
   *
   */
  public static void HydraTask_logManagerStatus() {
    CommandTestVersionHelper.logManagerStatus();
  }

}
