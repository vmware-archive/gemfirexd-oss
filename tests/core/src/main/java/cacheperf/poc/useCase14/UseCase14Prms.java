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

package cacheperf.poc.useCase14;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */

public class UseCase14Prms extends BasePrms {

  /**
   * (int)
   * The percentage of keys created in the push region.
   * Not for use with oneof or range.
   */
  public static Long pushPercentage;
  public static int getPushPercentage() {
    Long key = pushPercentage;
    int val = tab().intAt(key);
    if (val < 0 || val > 100) {
      String s = BasePrms.nameForKey(key) + " must be between 0 and 100";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(UseCase14Prms.class);
  }
  public static void main(String args[]) {
      dumpKeys();
  }
}
