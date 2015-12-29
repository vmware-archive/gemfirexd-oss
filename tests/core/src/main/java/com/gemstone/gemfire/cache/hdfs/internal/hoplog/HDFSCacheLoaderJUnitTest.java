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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.List;

import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;

/**
 * Tests that entries loaded from a cache loader are inserted in the HDFS queue 
 * but are not inserted in async queues. 
 * 
 * @author hemantb
 *
 */
public class HDFSCacheLoaderJUnitTest extends BaseHoplogTestCase {

  private static int totalEventsReceived = 0;
  protected void configureHdfsStoreFactory() throws Exception {
    hsf = this.cache.createHDFSStoreFactory();
    hsf.setHomeDir(testDataDir.toString());
    hsf.setHDFSEventQueueAttributes(new HDFSEventQueueAttributesFactory().setBatchTimeInterval(100000000).setBatchSizeMB(10000).create());
  }

  /**
   * Tests that entries loaded from a cache loader are inserted in the HDFS queue 
   * but are not inserted in async queues. 
   * @throws Exception
   */
  public void testCacheLoaderForAsyncQAndHDFS() throws Exception {
    
    final AsyncEventQueueStats hdfsQueuestatistics = ((AsyncEventQueueImpl)cache.
        getAsyncEventQueues().toArray()[0]).getStatistics();
    
    AttributesMutator am = this.region.getAttributesMutator();
    am.setCacheLoader(new CacheLoader() {
      private int i = 0;
      public Object load(LoaderHelper helper)
      throws CacheLoaderException {
        return new Integer(i++);
      }
      
      public void close() { }
    });
    
    
    
    String asyncQueueName = "myQueue";
    new AsyncEventQueueFactoryImpl(cache).setBatchTimeInterval(1).
    create(asyncQueueName, new AsyncEventListener() {
      
      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }

      @Override
      public boolean processEvents(List events) {
        totalEventsReceived += events.size();
        return true;
      }
    });
    am.addAsyncEventQueueId(asyncQueueName);
    
    region.put(1, new Integer(100));
    region.destroy(1);
    region.get(1);
    region.destroy(1);
    
    assertTrue("HDFS queue should have received four events. But it received " + 
        hdfsQueuestatistics.getEventQueueSize(), 4 == hdfsQueuestatistics.getEventQueueSize());
    assertTrue("HDFS queue should have received four events. But it received " + 
        hdfsQueuestatistics.getEventsReceived(), 4 == hdfsQueuestatistics.getEventsReceived());
    
    region.get(1);
    Thread.sleep(2000);
    
    assertTrue("Async queue should have received only 3 events. But it received " + 
        totalEventsReceived, totalEventsReceived == 3);
    assertTrue("HDFS queue should have received 5 events. But it received " + 
        hdfsQueuestatistics.getEventQueueSize(), 5 == hdfsQueuestatistics.getEventQueueSize());
    assertTrue("HDFS queue should have received 5 events. But it received " + 
        hdfsQueuestatistics.getEventsReceived(), 5 == hdfsQueuestatistics.getEventsReceived());
    
    
  }
  
}
