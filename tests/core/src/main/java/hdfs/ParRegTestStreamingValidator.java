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

package hdfs;

import java.io.IOException;
import java.io.FileNotFoundException;

import hydra.*;
import parReg.KnownKeysTest;
import parReg.ParRegTest;
import parReg.ParRegUtil;
import util.*;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 *  Generic utility methods for testing with Hoplogs/HDFS
 */

public class ParRegTestStreamingValidator extends ParRegTest {

/**
 * Initialize the single instance of this test class. If this
 * VM has already initialized its instance, then skip reinitializing.
 * Initialize consists of creating the original HDFS Region (so we have access to the HDFS machinery),
 * closing the region (but leaving the cache open) and creating a new validation region (which 
 * will be populated with entries from HFDS).
 * 
 */
   public synchronized static void HydraTask_initialize() {
     if (testInstance == null) {
       String regionConfigName = ConfigPrms.getRegionConfig();
       testInstance = new ParRegTestStreamingValidator();
       testInstance.initializeRegion(regionConfigName, getCachePrmsName());

       if (testInstance.isBridgeConfiguration) {
          BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
       }
     }
   } 

/**  ENDTASK to load a validation region with entries from the Hoplogs SequentialFile
 */
   public static void HydraTask_loadDataFromHDFS() {
      // todo@lhughes - don't hardcode the region name (get from TestConfig)
      HDFSUtil.loadDataFromHDFS(testInstance.aRegion, "partitionedRegion");
   }
}
