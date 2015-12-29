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
public class ExponentialGeneratorPrms extends BasePrms {

  static {
    setValues(ExponentialGeneratorPrms.class);
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Fraction of the dataset that should be accessed {@link
   * #exponentialPercentile} of the time. Defaults to 0.8571428571.
   */
  public static Long exponentialFraction;

  public static double getExponentialFraction() {
    Long key = exponentialFraction;
    return tasktab().doubleAt(key, tab().doubleAt(key, 0.8571428571d));
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Percentage of the readings that should be within the most recent
   * {@link #exponentialFraction} portion of the dataset. Defaults to 95.
   */
  public static Long exponentialPercentile;

  public static double getExponentialPercentile() {
    Long key = exponentialPercentile;
    return tasktab().doubleAt(key, tab().doubleAt(key, 95));
  }
}
