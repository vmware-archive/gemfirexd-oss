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

package hct.multiDS;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */
public class MultiDSPrms extends BasePrms {

  /**
   * (String)
   * Name of the distributed system of interest.
   */
  public static Long ds;
  public static String getDS() {
    Long key = ds;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * (String)
   * Name of the region of interest.
   */
  public static Long regionName;
  public static String getRegionName() {
    Long key = regionName;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
  /**
   * (int)
   * Number of keys to put in each region.  Defaults to 10000.
   */
  public static Long numKeys;
  public static int getNumKeys() {
    Long key = numKeys;
    return tab().intAt(key, 10000);
  }

//----------------------------------------------------------------------------
  static {
    setValues(MultiDSPrms.class);
  }
}
