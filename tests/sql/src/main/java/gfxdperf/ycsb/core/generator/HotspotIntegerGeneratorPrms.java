/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
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
package gfxdperf.ycsb.core.generator;

import hydra.BasePrms;
import hydra.HydraConfigException;

/**
 * A class used to store keys for test configuration settings.
 */
public class HotspotIntegerGeneratorPrms extends BasePrms {

  static {
    setValues(HotspotIntegerGeneratorPrms.class);
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Fraction of the dataset that constitute the hot set. Defaults to 0.2.
   */
  public static Long hotspotDataFraction;

  public static double getHotspotDataFraction() {
    Long key = hotspotDataFraction;
    return getFraction(key, 0.2d);
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Fraction of operations that access the hot set. Defaults to 0.8.
   */
  public static Long hotspotOperationFraction;

  public static double getHotspotOperationFraction() {
    Long key = hotspotOperationFraction;
    return getFraction(key, 0.8d);
  }

//------------------------------------------------------------------------------

  private static double getFraction(Long key, double defaultVal) {
    double val = tasktab().doubleAt(key, tab().doubleAt(key, defaultVal));
    if (val < 0.0 || val > 1.0) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }
}
