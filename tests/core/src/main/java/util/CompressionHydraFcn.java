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

package util;

import java.util.Random;

import hydra.TestConfig;

/**
 * @author lynn
 *
 */
public class CompressionHydraFcn {

  /**
   *  Randomize the value of hydra.RegionPrms-compressor between the given compressor
   *  and none.
   *  
   *  Snappy is not supported on Solaris, so for tests that want to randomize this setting
   *  it must always return none for Solaris, then truly randomize for other hosts.
   *  
   *  validCompressor The value to randomize with "none".
   */
  public static String randomCompressor(String validCompressor) {
    if (System.getProperty("os.name").equals("SunOS")) { // is Solaris
      return "none";
    } else {
      Random rand = new Random();
      if (rand.nextBoolean()) {
        return validCompressor;
      } else {
        return "none";
      }
    }
  }

}
