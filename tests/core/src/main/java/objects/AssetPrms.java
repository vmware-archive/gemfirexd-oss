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

package objects;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */

public class AssetPrms extends BasePrms {

  public static final double DEFAULT_MAX_VALUE = 0.0d;

  /**
   * (double)
   * Maximum value of the asset.  Defaults to {@link #DEFAULT_MAX_VALUE}.
   * The asset value is computed as a random value between 1 and this value
   * (inclusive).
   */
  public static Long maxValue;

  public static double getMaxValue() {
    Long key = maxValue;
    return tab().doubleAt(key, DEFAULT_MAX_VALUE);
  }

  static {
    setValues(AssetPrms.class);
  }
  public static void main( String args[] ) {
    dumpKeys();
  }
}
