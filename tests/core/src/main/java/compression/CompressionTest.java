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
package compression;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.Region;

import hydra.Log;
import util.RandomValues;
import util.TestHelper;

/**
 * The CompressionTest class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class CompressionTest {

  // single static instance of the test class
  public static CompressionTest testInstance;

  protected List<Region> theRegions;
  protected RandomValues randomValues;

  public static void HydraTask_endTask() {
    Log.getLogWriter().info("CompressionTest.HydraTask_endTask.");
  }
  protected void initialize() {
    Log.getLogWriter().info("CompressionTest.initialize.");

    if (testInstance == null) {
      testInstance = new CompressionTest();
    }
    testInstance.theRegions = new ArrayList<Region>();
    testInstance.randomValues = new RandomValues();
  }

  protected void logRegionHierarchy() {
    Log.getLogWriter().info("CompressionTest.logRegionHierarchy about to TestHelper.logRegionHierarchy().");
    TestHelper.logRegionHierarchy();
  }

}
