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

package com.gemstone.gemfire.cache.hdfs.internal;

import org.apache.hadoop.conf.Configuration;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.StoreExistsException;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.DDLHoplogOrganizer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;


/**
 * Implementation of HDFSStoreFactory 
 * 
 * @author Hemant Bhanawat
 */
public class HDFSStoreFactoryImpl extends HDFSStoreCreation {
  public static final String DEFAULT_ASYNC_QUEUE_ID_FOR_HDFS= "GEMFIRE_HDFS_BUCKETSORTED_QUEUE";
  
  private Cache cache;
  private Configuration conf;
  
  public HDFSStoreFactoryImpl(Cache cache) {
    this(cache, null);
  }
  
  public HDFSStoreFactoryImpl(Cache cache, HDFSStoreCreation config) {
    super(config);
    this.cache = cache;
  }

  @Override
  public HDFSStore create(String name) {
    if (name == null) {
      throw new GemFireConfigException("HDFS store name not provided");
    }
    
    HDFSStore result = null;
    synchronized (this) {
      if (this.cache instanceof GemFireCacheImpl) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) this.cache;
        if (gfc.findHDFSStore(name) != null) {
          throw new StoreExistsException(name);
        }
        
        HDFSStoreImpl hsi = new HDFSStoreImpl(name, this.configHolder);
        DDLHoplogOrganizer ddlOrganizer = new DDLHoplogOrganizer(hsi);
        hsi.setDDLHoplogOrganizer(ddlOrganizer);
        gfc.addHDFSStore(hsi);
        result = hsi;
      }
    }
    return result;
  }

  public static final String getEventQueueName(String regionPath) {
    return HDFSStoreFactoryImpl.DEFAULT_ASYNC_QUEUE_ID_FOR_HDFS + "_"
        + regionPath.replace('/', '_');
  }

  public Configuration getConfiguration() {
    return conf;
  }
  
  public HDFSStoreFactory setConfiguration(Configuration conf) {
    this.conf = conf;
    return this;
  }
 
  public HDFSStore getConfigView() {
    return (HDFSStore) configHolder;
  }
}
