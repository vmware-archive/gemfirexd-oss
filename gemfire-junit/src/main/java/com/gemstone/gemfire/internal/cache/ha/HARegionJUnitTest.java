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
package com.gemstone.gemfire.internal.cache.ha;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;

/**
 * Test verifies the properties of a HARegion which allows localPuts and
 * localDestroys on a MirroredRegion
 * 
 * @author Mitul Bid
 *  
 */
public class HARegionJUnitTest extends TestCase
{

  /**
   * constructor
   * 
   * @param arg0
   */
  public HARegionJUnitTest(String arg0) {
    super(arg0);
  }

  /**
   * create the cache
   */
  protected void setUp() throws Exception
  {
    super.setUp();
    cache = createCache();
  }

  /**
   * close the cache in tear down
   */
  protected void tearDown() throws Exception
  {
    super.tearDown();
    cache.close();
  }

  /**
   * cache
   */
  private Cache cache = null;

  /**
   * 
   * create the cache
   * 
   * @return @throws
   *         TimeoutException
   * @throws CacheWriterException
   * @throws CapacityControllerException
   * @throws GatewayException
   * @throws CacheExistsException
   * @throws RegionExistsException
   */
  private Cache createCache() throws TimeoutException, CacheWriterException,
       GatewayException, CacheExistsException,
      RegionExistsException
  {
    return CacheFactory.create(DistributedSystem.connect(new Properties()));
  }

  /**
   * create the HARegion
   * 
   * @return @throws
   *         TimeoutException
   * @throws CacheWriterException
   * @throws CapacityControllerException
   * @throws GatewayException
   * @throws CacheExistsException
   * @throws RegionExistsException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private Region createHARegion() throws TimeoutException,
      CacheWriterException,  GatewayException,
      CacheExistsException, RegionExistsException, IOException,
      ClassNotFoundException
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ExpirationAttributes ea = new ExpirationAttributes(2000,
        ExpirationAction.LOCAL_INVALIDATE);
    factory.setStatisticsEnabled(true);
    ;
    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterInvalidate(EntryEvent event)
      {
      }
    });
    RegionAttributes ra = factory.create();
    Region region = HARegion.getInstance("HARegionJUnitTest_region", (GemFireCacheImpl)cache,
        null, ra);
    region.getAttributesMutator().setEntryTimeToLive(ea);
    return region;

  }

  /**
   * test no exception being thrown while creating an HARegion
   *  
   */
  public void testRegionCreation()
  {
    try {
      createHARegion();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to " + e);
    }
  }

  /**
   * test no exception being thrown while put is being done on an HARegion
   *  
   */
  public void testPut()
  {
    try {
      Region region = createHARegion();
      region.put("key1", "value1");
      Assert.assertEquals(region.get("key1"), "value1");
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
  }

  /**
   * test no exception being thrown while doing a localDestroy on a HARegion
   *  
   */
  public void testLocalDestroy()
  {
    try {
      Region region = createHARegion();
      region.put("key1", "value1");
      region.localDestroy("key1");
      Assert.assertEquals(region.get("key1"), null);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("put failed due to " + e);
    }
  }
  /**
   * Test to verify event id exists when evict destroy happens.
   * 
   */
  public void testEventIdSetForEvictDestroy()
  { 
    try{
      AttributesFactory factory = new AttributesFactory();    
      
      factory.setCacheListener(new CacheListenerAdapter(){        
        public void afterDestroy(EntryEvent event){          
          assertTrue("eventId has not been set for "+ event, ((EntryEventImpl)event).getEventId() != null);          
        }
       });
      
      EvictionAttributes evAttr = EvictionAttributes.createLRUEntryAttributes(1,EvictionAction.LOCAL_DESTROY);
      factory.setEvictionAttributes(evAttr);   
            
      RegionAttributes attrs = factory.createRegionAttributes();
      Region region = cache.createVMRegion("TEST_REGION", attrs);
      region.put("key1", "value1");
      region.put("key2", "value2");
    }
    catch (Exception e) {      
    }
    
    
  }

}
