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

package hydratest.hdfs;

import com.gemstone.gemfire.cache.*;
import hydra.*;

/**
 * Tasks for testing hydra hdfs support.
 */
public class HDFSClient {

  public static void configureHadoopTask() {
    HadoopHelper.configureHadoop(ConfigPrms.getHadoopConfig());
  }

  public static void formatNameNodesTask() {
    HadoopHelper.formatNameNodes(ConfigPrms.getHadoopConfig());
  }

  public static void startNameNodesTask() {
    HadoopHelper.startNameNodes(ConfigPrms.getHadoopConfig());
  }

  public static void startDataNodesTask() {
    HadoopHelper.startDataNodes(ConfigPrms.getHadoopConfig());
  }

  public static void stopNameNodesTask() {
    HadoopHelper.stopNameNodes(ConfigPrms.getHadoopConfig());
  }

  public static void stopDataNodesTask() {
    HadoopHelper.stopDataNodes(ConfigPrms.getHadoopConfig());
  }

  public static void startResourceManagerTask() {
    HadoopHelper.startResourceManager(ConfigPrms.getHadoopConfig());
  }

  public static void startNodeManagersTask() {
    HadoopHelper.startNodeManagers(ConfigPrms.getHadoopConfig());
  }

  public static void stopResourceManagerTask() {
    HadoopHelper.stopResourceManager(ConfigPrms.getHadoopConfig());
  }

  public static void stopNodeManagersTask() {
    HadoopHelper.stopNodeManagers(ConfigPrms.getHadoopConfig());
  }

  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  public static void startLocatorTask() {
    DistributedSystemHelper.startLocatorAndDS();
  }

  public static void createCacheTask() {
    String cc = ConfigPrms.getCacheConfig();
    Cache cache = CacheHelper.createCache(cc);
  }

  public static void createRegionTask() {
    String rc = ConfigPrms.getRegionConfig();
    RegionHelper.createRegion(rc);
  }

  public static void putDataTask() {
    String rc = ConfigPrms.getRegionConfig();
    String regionName = RegionHelper.getRegionDescription(rc).getRegionName();
    Region r = RegionHelper.getRegion(regionName);
    for (int i = 0; i < 10000; i++) {
      r.put(Integer.valueOf(i), String.valueOf(i));
    }
  }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }
}
