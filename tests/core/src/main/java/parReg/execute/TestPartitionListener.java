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
package parReg.execute;


import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionListenerAdapter;
import com.gemstone.gemfire.cache.partition.PartitionManager;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class TestPartitionListener extends PartitionListenerAdapter implements
    Serializable, Declarable2 {

  public static final int NUM_COLOCATED_REGIONS = 7;

  private final Properties listenerProps;
  
  private Cache theCache;

  public TestPartitionListener() {
    this.listenerProps = new Properties();
    this.listenerProps.setProperty("listenerType", "pr_listener");
  }

  public void afterPrimary(int id) {
    for (int i = 2; i <= NUM_COLOCATED_REGIONS; i++) {
      PartitionedRegion viewPR = (PartitionedRegion)theCache
          .getRegion(Region.SEPARATOR + "clientRegion" + i);
      hydra.Log.getLogWriter().info(
          "Creating bucket with id " + id + " for the region "
              + viewPR.getName());
      PartitionManager.createPrimaryBucket(viewPR, id, true, true);
    }
  }

  public Properties getConfig() {
    return this.listenerProps;
  }

  public void init(Properties props) {
    this.listenerProps.putAll(props);
  }

  public void afterRegionCreate(Region region) {
    theCache = region.getCache();
  }

}
