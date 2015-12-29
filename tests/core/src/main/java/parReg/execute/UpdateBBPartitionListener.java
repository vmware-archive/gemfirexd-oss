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

import getInitialImage.InitImageBB;
import hydra.RemoteTestModule;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class UpdateBBPartitionListener implements PartitionListener,
    Serializable, Declarable2 {

  static ArrayList bucketIds = new ArrayList();
  
  public static void setBucketIds(ArrayList bucketIds) {
    UpdateBBPartitionListener.bucketIds = bucketIds;
  }

  public static void setRecreatedBuckets(HashSet recreatedBuckets) {
    UpdateBBPartitionListener.recreatedBuckets = recreatedBuckets;
  }

  static HashSet recreatedBuckets = new HashSet();
  
  private final Properties listenerProps;
  
  public UpdateBBPartitionListener() {
    this.listenerProps = new Properties();
    this.listenerProps.setProperty("listenerType", "pr_listener");
  }

  public List getBucketIdsList() {
    return bucketIds;
  }
  
  public Set getReCreatedBucketSet() {
    return recreatedBuckets;
  }

  public void afterPrimary(int id) {
    hydra.Log.getLogWriter().info("in afterPrimary with id " + id);
    synchronized (bucketIds) {
      bucketIds.add(id);
    }
    synchronized (InitImageBB.getBB().getSharedMap()) {
      if (InitImageBB.getBB().getSharedMap().get("BUCKET_" + id) != null) {
        recreatedBuckets.add(id);
      }

      InitImageBB.getBB().getSharedMap().put("BUCKET_" + id,
          new Integer(RemoteTestModule.getMyVmid()));
    }
  }


  public void afterRegionCreate(Region region) {
  }

  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
  }
  
  public void afterBucketCreated(int bucketId, Iterable<?> keys) {
  }

  public Properties getConfig() {
    return this.listenerProps;
  }

  public void init(Properties props) {
    this.listenerProps.putAll(props);
  }

}
