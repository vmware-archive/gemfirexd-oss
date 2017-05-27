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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueConfigurationException;
import junit.framework.TestCase;

/**
 * @author skumar
 *
 */
public class AsyncEventQueueValidationsJUnitTest extends TestCase {

  private Cache cache;
  
  /**
   * @param name
   */
  public AsyncEventQueueValidationsJUnitTest(String name) {
    super(name);
  }

  public void testConcurrentParallelAsyncEventQueueAttributesWrongDispatcherThreads() {
    cache = new CacheFactory().create();
    try {
      AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory();
      fact.setParallel(true);
      fact.setDispatcherThreads(-5);
      fact.setOrderPolicy(OrderPolicy.KEY);
      fact.create("id", new com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
        assertTrue(e.getMessage()
            .contains(" can not be created with dispatcher threads less than 1"));
    }
  }
  
  
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyThread() {
    cache = new CacheFactory().create();
    try {
      AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory();
      fact.setParallel(true);
      fact.setDispatcherThreads(5);
      fact.setOrderPolicy(OrderPolicy.THREAD);
      fact.create("id", new com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
        assertTrue(e.getMessage()
            .contains("can not be created with OrderPolicy"));
    }
  }
  
  
}
