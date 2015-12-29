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

package newWan.serial.filters;

import hydra.*;

/**
 * A class used to store keys for wan filter configuration.
 */

public class WanFilterTestPrms extends BasePrms {

  /**
   * (int)
   * Number of keys to work on in each task to carry out on each key.  Defaults to 20.
   */
  public static Long numKeysPerTask;
  public static int getNumKeysPerTask() {
    Long key = numKeysPerTask;
    int val = tasktab().intAt( key, tab().intAt( key, 20 ) );
    return val;
  }
    
  static {
    setValues(WanFilterTestPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}

