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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.util.BridgeWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import io.snappydata.test.dunit.DistributedTestBase;
import junit.framework.TestCase;

/**
 * Tests the proper intialization of redundancyLevel property.
 * 
 */
public class RedundancyLevelJUnitTest extends TestCase
{
  
  final String expected = "Could not initialize a primary queue on startup. No queue servers available";
  
  /** The distributed system */
  DistributedSystem system;

  /** The distributed system */
  Cache cache;

  /** The proxy instance */
  ConnectionProxy proxy = null;

  /**
   * Close the cache and proxy instances for a test and disconnect from the
   * distributed system.
   */
  protected void tearDown() throws Exception
  {
    if (cache != null) {
      cache.close();
    }
    if (system != null) {
      system.disconnect();
    }
    if (proxy != null) {
      proxy.close();
    }
    super.tearDown();
  }

  /**
   * Tests that value for redundancyLevel of the failover set is correctly
   * picked via cache-xml file.(Please note that the purpose of this test is to
   * just verify that the value is initialized correctly from cache-xml and so
   * only client is started and the connection-exceptions due to no live
   * servers, which appear as warnings, are ignored.)
   * 
   * @author Dinesh Patel
   * 
   */
  public void testRedundancyLevelSetThroughXML()
  {
    try {
      // System.setProperty("JTESTS", "U:/tests");
      String path = System.getProperty("JTESTS") + "/lib/redundancylevel.xml";

      Properties p = new Properties();
      p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      p.setProperty(DistributionConfig.LOCATORS_NAME, "");
      p.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, path);
      final String addExpected =
        "<ExpectedException action=add>" + expected + "</ExpectedException>";
      system = DistributedSystem.connect(p);
      system.getLogWriter().info(addExpected);
     
      try {
        
        cache = CacheFactory.create(system);
        assertNotNull("cache was null", cache);
        Region region = cache.getRegion("/root/exampleRegion");
        assertNotNull(region);
        BridgeWriter writer = (BridgeWriter)region.getAttributes()
            .getCacheWriter();
        Pool pool = (Pool)writer.getConnectionProxy();
        assertEquals(
            "Redundancy level not matching the one specified in cache-xml", 6,
            pool.getSubscriptionRedundancy());
      } finally {
        final String removeExpected =
          "<ExpectedException action=remove>" + expected + "</ExpectedException>";
        cache.getLogger().info(removeExpected);
      }
    }
    catch (Exception ex) {
      fail("Test failed due to " + DistributedTestBase.getStackTrace(ex));
    }
  }

}
