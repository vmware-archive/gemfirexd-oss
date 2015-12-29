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

package com.gemstone.gemfire.cache.client;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;

import static com.gemstone.gemfire.cache.client.ClientRegionShortcut.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.internal.ProxyRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.DistributedSystem;

import junit.framework.TestCase;

/**
 * Unit test for the ClientRegionFactory class
 * @author darrel
 * @since 6.5
 */
public class ClientRegionFactoryJUnitTest extends TestCase
{
  public static final String key = "key";
  public static final Integer val = new Integer(1);
  final String r1Name = "r1";
  final String sr1Name = "sr1";
  final String r2Name = "r2";
  final String r3Name = "r3";
  public Cache cache = null;

  public DistributedSystem distSys = null;

  private void cleanUpRegion(Region r) {
    if (r != null && !r.getCache().isClosed() && !r.isDestroyed()
        && r.getCache().getDistributedSystem().isConnected()) {
      this.cache = r.getCache();
      this.distSys = this.cache.getDistributedSystem();
      r.localDestroyRegion();
    }
  }

  protected void tearDown() throws Exception
  {
    Cache c = this.cache;
    DistributedSystem d = this.distSys;
    if (c != null && !c.isClosed()) {
      d = c.getDistributedSystem();
      c.close();
    }
    if (d != null && d.isConnected()) {
      d.disconnect();
    }
  }

  public void assertBasicRegionFunctionality(Region r, String name)
  {
    assertEquals(r.getName(), name);
    r.put(key, val);
    assertEquals(r.getEntry(key).getValue(), val);
  }

  public static void assertEquals(RegionAttributes ra1, RegionAttributes ra2)
  {
    assertEquals(ra1.getScope(), ra2.getScope());
    assertEquals(ra1.getKeyConstraint(), ra2.getKeyConstraint());
    assertEquals(ra1.getValueConstraint(), ra2.getValueConstraint());
    assertEquals(ra1.getCacheListener(), ra2.getCacheListener());
    assertEquals(ra1.getCacheWriter(), ra2.getCacheWriter());
    assertEquals(ra1.getCacheLoader(), ra2.getCacheLoader());
    assertEquals(ra1.getStatisticsEnabled(), ra2.getStatisticsEnabled());
    assertEquals(ra1.getConcurrencyLevel(), ra2.getConcurrencyLevel());
    assertEquals(ra1.getInitialCapacity(), ra2.getInitialCapacity());
    assertTrue(ra1.getLoadFactor() == ra2.getLoadFactor());
    assertEquals(ra1.getEarlyAck(), ra2.getEarlyAck());
    assertEquals(ra1.isDiskSynchronous(), ra2.isDiskSynchronous());
    assertEquals(ra1.getDiskStoreName(), ra2.getDiskStoreName());
  }

//   /*
//    * Test method for 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory()'
//    */
//   public void testRegionFactory() throws CacheException
//   {
//     // Assert basic region creation when no DistributedSystem or Cache exists
//     Region r1 = null, r2 = null;
//     try {
//       RegionFactory factory = new RegionFactory();
//       r1 = factory.create(r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);

//       // Assert duplicate creation failure
//       try {
//         factory.create(r1Name);
//       }
//       catch (RegionExistsException expected) {
//       }

//       r2 = factory.create(r2Name);
//       assertBasicRegionFunctionality(r2, r2Name);
//       r2.destroyRegion();

//       // as of 6.5 if the cache that was used to create a regionFactory is closed
//       // then the factory is out of business
//       // @todo add a test to check this
// //       // Assert we can handle a closed Cache
// //       Cache c = r1.getCache();
//       r1.destroyRegion();
// //       c.close();
//       r1 = factory.create(r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);

//       // as of 6.5 if the ds that was used to create a regionFactory is disconnected
//       // then the factory is out of business
//       // @todo add a test to check this
// //       // Assert we can handle a disconnected disributed system
// //       DistributedSystem d = r1.getCache().getDistributedSystem();
//       r1.destroyRegion();
// //       d.disconnect();
//       r1 = factory.create(r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);

//       // as of 6.5 if the ds that was used to create a regionFactory is disconnected
//       // then the factory is out of business
//       // @todo add a test to check this
//       // Assert we can handle both a closed Cache and a disconnected system
// //       c = r1.getCache();
// //       d = c.getDistributedSystem();
//       r1.destroyRegion();
// //       c.close();
// //       d.disconnect();
//       r1 = factory.create(r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);
//     }
//     finally {
//       cleanUpRegion(r1);
//       cleanUpRegion(r2);
//       try {
//         tearDown();
//       } catch (Exception tearDownBummer) {
//         fail("Problem cleaning up: " + tearDownBummer);
//       }
//     }
    
//     // Assert basic region creation when a Distributed system exists
//     try {
//       this.distSys = DistributedSystem.connect(new Properties()); // for teardown 

//       RegionFactory factory = new RegionFactory();
//       r1 = factory.create(r1Name);
//       this.cache = r1.getCache(); // for teardown 
//       assertBasicRegionFunctionality(r1, r1Name);
//     }
//     finally {
//       cleanUpRegion(r1);
//       try {
//         tearDown();
//       } catch (Exception tearDownBummer) {
//         fail("Problem cleaning up: " + tearDownBummer);
//       }
//     }

//     // Assert failure when a Distributed system exists but with different properties 
//     try {
//       this.distSys = DistributedSystem.connect(new Properties()); // for teardown 
//       final Properties failed = new Properties();
//       failed.put("mcast-ttl", "64");

//       new RegionFactory(failed);
//       fail("Expected exception");
//     } catch (IllegalStateException expected) {
//     }
//     finally {
//       cleanUpRegion(r1);
//       try {
//         tearDown();
//       } catch (Exception tearDownBummer) {
//         fail("Problem cleaning up: " + tearDownBummer);
//       }
//     }

//     // Assert basic region creation when a Distributed and Cache exist
//     try {
//       DistributedSystem ds = DistributedSystem.connect(new Properties());
//       CacheFactory.create(ds);

//       RegionFactory factory = new RegionFactory();
//       r1 = factory.create(r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);
//     }
//     finally {
//       cleanUpRegion(r1);
//       try {
//         tearDown();
//       } catch (Exception tearDownBummer) {
//         fail("Problem cleaning up: " + tearDownBummer);
//       }
//     }
    
//     // Assert basic region creation when a Distributed and Cache exist but the cache is closed
//     try {
//       DistributedSystem ds = DistributedSystem.connect(new Properties());
//       this.cache = CacheFactory.create(ds);
//       this.cache.close();

//       RegionFactory factory = new RegionFactory();
//       r1 = factory.create(r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);
//     }
//     finally {
//       cleanUpRegion(r1);
//       try {
//         tearDown();
//       } catch (Exception tearDownBummer) {
//         fail("Problem cleaning up: " + tearDownBummer);
//       }
//     }

//   }

//   /*
//    * Test method for
//    * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(RegionAttributes)'
//    */
//   public void testRegionFactoryRegionAttributes() throws CacheException
//   {
//     Region r1 = null, r2 = null, r3 = null;
//     try {
//       Properties p = new Properties();
//       p.put("mcast-port", "0");
//     r1 = new RegionFactory(p).setScope(Scope.LOCAL)
//         .setConcurrencyLevel(1).setLoadFactor(0.8F).setKeyConstraint(
//             String.class).setStatisticsEnabled(true).create(r1Name);
//     assertBasicRegionFunctionality(r1, r1Name);

//     final RegionFactory factory = new RegionFactory(p, r1.getAttributes());
//     r2 = factory.create(r2Name);
//     assertBasicRegionFunctionality(r2, r2Name);
//     assertEquals(r1.getAttributes(), r2.getAttributes());

//     r3 = factory.create(r3Name);
//     try {
//       assertEquals(r2.getAttributes(), r3.getAttributes());
//       fail("Expected r2 attributes to be different from r3");
//     }
//     catch (AssertionFailedError expected) {
//     }
//     } finally {
//       cleanUpRegion(r1);
//       cleanUpRegion(r2);
//       cleanUpRegion(r3);
//     }
//   }
  

//   /*
//    * Test method for
//    * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(String)'
//    */
//   public void testRegionFactoryString() throws CacheException, IOException
//   {

//     DistributionConfig.DEFAULT_CACHE_XML_FILE.delete();
//     Region r1 = null;
//     try {
//       DistributionConfig.DEFAULT_CACHE_XML_FILE.createNewFile();
//       FileWriter f = new FileWriter(DistributionConfig.DEFAULT_CACHE_XML_FILE);
//       f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
//               + "<!DOCTYPE cache PUBLIC\n  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN\"\n"
//               + "  \"http://www.gemstone.com/dtd/cache6_5.dtd\">\n"
//               + "<cache>\n"
//               + " <region-attributes id=\""
//               + getName()
//               + "\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n"
//               + "  <key-constraint>"
//               + String.class.getName()
//               + "</key-constraint>\n"
//               + "  <value-constraint>"
//               + Integer.class.getName()
//               + "</value-constraint>\n"
//               + "    <entry-idle-time><expiration-attributes timeout=\"60\"/></entry-idle-time>\n"
//               + " </region-attributes>\n" + "</cache>");
//       f.close();

//       RegionFactory factory = new RegionFactory(getName());
//       r1 = factory.create(this.r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);
//       RegionAttributes ra = r1.getAttributes();
//       assertEquals(ra.getStatisticsEnabled(), true);
//       assertEquals(ra.getScope().isDistributedAck(), true);
//       assertEquals(ra.getValueConstraint(), Integer.class);
//       assertEquals(ra.getKeyConstraint(), String.class);
//       assertEquals(ra.getEntryIdleTimeout().getTimeout(), 60);
//     }
//     finally {
//       DistributionConfig.DEFAULT_CACHE_XML_FILE.delete();
//       cleanUpRegion(r1);
//     }
//   }

//   /*
//    * Test method for
//    * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(Properties)'
//    */
//   public void testRegionFactoryProperties() throws CacheException, IOException
//   {
//     Region r1 = null;
//     try {
//       final Properties props1 = new Properties();
//       props1.put("mcast-ttl", "64");
//       RegionFactory factory = new RegionFactory(props1);
//       r1 = factory.create(this.r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);
//       assertEquals(props1.get("mcast-ttl"), 
//           r1.getCache().getDistributedSystem().getProperties().get("mcast-ttl"));
//     } finally {
//       cleanUpRegion(r1);
//     }
//   }

//   /*
//    * Test method for
//    * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(Properties,
//    * RegionAttributes)'
//    */
//   public void testRegionFactoryPropertiesRegionAttributes()
//   {

//   }

//   /*
//    * Test method for
//    * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(Properties,
//    * String)'
//    */
//   public void testRegionFactoryPropertiesString() throws IOException, CacheException
//   {
//     Region r1 = null;
//     File xmlFile = null;
//     try {
//       final Properties props2 = new Properties();
//       props2.put("mcast-ttl", "64");
//       final String xmlFileName = getName() + "-cache.xml";
//       props2.put("cache-xml-file", xmlFileName);
//       xmlFile = new File(xmlFileName);
//       xmlFile.delete();
//       xmlFile.createNewFile();
//       FileWriter f = new FileWriter(xmlFile);
//       final String attrsId = getName() + "-attrsId"; 
//       f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
//               + "<!DOCTYPE cache PUBLIC\n  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN\"\n"
//               + "  \"http://www.gemstone.com/dtd/cache6_5.dtd\">\n"
//               + "<cache>\n"
//               + " <region-attributes id=\""
//               + attrsId
//               + "\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n"
//               + "  <key-constraint>"
//               + String.class.getName()
//               + "</key-constraint>\n"
//               + "  <value-constraint>"
//               + Integer.class.getName()
//               + "</value-constraint>\n"
//               + "    <entry-idle-time><expiration-attributes timeout=\"60\"/></entry-idle-time>\n"
//               + " </region-attributes>\n" + "</cache>");
//       f.close();

//       RegionFactory factory = new RegionFactory(props2, attrsId);
//       r1 = factory.create(this.r1Name);
//       assertBasicRegionFunctionality(r1, r1Name);
//       assertEquals(props2.get("mcast-ttl"), 
//           r1.getCache().getDistributedSystem().getProperties().get("mcast-ttl"));
//       assertEquals(props2.get("cache-xml-file"), 
//           r1.getCache().getDistributedSystem().getProperties().get("cache-xml-file"));
//       RegionAttributes ra = r1.getAttributes();
//       assertEquals(ra.getStatisticsEnabled(), true);
//       assertEquals(ra.getScope().isDistributedAck(), true);
//       assertEquals(ra.getValueConstraint(), Integer.class);
//       assertEquals(ra.getKeyConstraint(), String.class);
//       assertEquals(ra.getEntryIdleTimeout().getTimeout(), 60);
//     } finally {
//       if (xmlFile != null) {
//         xmlFile.delete();
//       }
//       cleanUpRegion(r1);
//     }

//   }
  
  /**
   * Ensure that the RegionFactory set methods mirrors those found in RegionAttributes
   * 
   * @throws Exception
   */
  public void testAttributesFactoryConformance() throws Exception {
    Method[] af = AttributesFactory.class.getDeclaredMethods();
    Method[] rf = ClientRegionFactory.class.getDeclaredMethods();
    Method am, rm;
    
    ArrayList afDeprected = new ArrayList(); // hack to ignore deprecated methods
    afDeprected.add("setCacheListener");
    afDeprected.add("setMirrorType");
    afDeprected.add("setPersistBackup");
    afDeprected.add("setBucketRegion");    
    afDeprected.add("setEnableWAN");    
    afDeprected.add("setEnableBridgeConflation");    
    afDeprected.add("setEnableConflation");
    afDeprected.add("setEarlyAck");
    afDeprected.add("setPublisher");
    afDeprected.add("setDiskWriteAttributes");
    afDeprected.add("setDiskDirs");
    afDeprected.add("setDiskDirsAndSizes");
    // the following are not supported on client
    afDeprected.add("setCacheWriter");
    afDeprected.add("setCacheLoader");
    afDeprected.add("setScope");
    afDeprected.add("setDataPolicy");
    afDeprected.add("setEnableGateway");
    afDeprected.add("setGatewayHubId");
    afDeprected.add("setEnableAsyncConflation");
    afDeprected.add("setEnableSubscriptionConflation");
    afDeprected.add("setPartitionAttributes");
    afDeprected.add("setMembershipAttributes");
    afDeprected.add("setSubscriptionAttributes");
    afDeprected.add("setIndexMaintenanceSynchronous");
    afDeprected.add("setIgnoreJTA");
    afDeprected.add("setLockGrantor");
    afDeprected.add("setMulticastEnabled");
    afDeprected.add("addGatewaySenderId");
    afDeprected.add("addAsyncEventQueueId");
    afDeprected.add("setHDFSStoreName");
    afDeprected.add("setHDFSWriteOnly");
    afDeprected.add("setEnableOffHeapMemory");
    
    ArrayList methodsToImplement = new ArrayList();

    // Since the ClientRegionFactory has an AttributesFactory member,
    // we only need to make sure the ClientRegionFactory class adds proxies for the
    // 'set' and 'add' methods added to the AttributesFactory. The java complier
    // will notify the
    // developer if a method is removed from AttributesFactory.
    String amName;
    boolean hasMethod = false;
    assertTrue(af.length != 0);
    for (int i=0; i<af.length; i++) {
      am = af[i];
      amName = am.getName();
      if (!afDeprected.contains(amName) && 
          (amName.startsWith("set") || amName.startsWith("add"))) {
        for (int j=0; j<rf.length; j++) {
          rm = rf[j];
          if (rm.getName().equals(am.getName())) {
            Class[] rparams = rm.getParameterTypes();
            Class[] aparams = am.getParameterTypes();
            if (rparams.length == aparams.length) {
              boolean hasAllParams = true;
              for (int k = 0; k < rparams.length; k++) {
                if (aparams[k] != rparams[k]) {
                  hasAllParams = false;
                  break;
                }
              } // parameter check
              if (hasAllParams) {
                hasMethod = true;
              }
            } 
          } 
        } // region factory methods
        if (!hasMethod) {
          methodsToImplement.add(am);
        } else {
          hasMethod = false;
        }
      }
    } // attributes methods
    
    if (methodsToImplement.size() > 0) {
      fail("ClientRegionFactory does not conform to AttributesFactory, its should proxy these methods " + methodsToImplement);                
    }
  }

  public void testLOCAL() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(null, ra.getPoolName());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_HEAP_LRU() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_HEAP_LRU);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(null, ra.getPoolName());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_OVERFLOW() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(null, ra.getPoolName());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_PERSISTENT() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(null, ra.getPoolName());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_PERSISTENT_OVERFLOW() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(null, ra.getPoolName());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPROXY() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals("DEFAULT", ra.getPoolName());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testCACHING_PROXY() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals("DEFAULT", ra.getPoolName());
      assertEquals(0,
                   (int)c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testCACHING_PROXY_LRU() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY_HEAP_LRU);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals("DEFAULT", ra.getPoolName());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testCACHING_PROXY_OVERFLOW() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals("DEFAULT", ra.getPoolName());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testAddCacheListener() throws CacheException, IOException {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
      CacheListener cl = new MyCacheListener();
      r1 = factory.addCacheListener(cl).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(cl, ra.getCacheListener());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testInitCacheListener() throws CacheException, IOException {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
      CacheListener cl1 = new MyCacheListener();
      CacheListener cl2 = new MyCacheListener();
      r1 = factory.initCacheListeners(new CacheListener[] {cl1, cl2}).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, Arrays.equals(new CacheListener[] {cl1, cl2}, ra.getCacheListeners()));
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetEvictionAttributes() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(77)).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(EvictionAttributes.createLRUEntryAttributes(77), ra.getEvictionAttributes());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetEntryIdleTimeout() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      ExpirationAttributes ea = new ExpirationAttributes(7);
      r1 = factory.setEntryIdleTimeout(ea).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ea, ra.getEntryIdleTimeout());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetCustomEntryIdleTimeout() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      MyCustomExpiry ce = new MyCustomExpiry();
      r1 = factory.setCustomEntryIdleTimeout(ce).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ce, ra.getCustomEntryIdleTimeout());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetEntryTimeToLive() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      ExpirationAttributes ea = new ExpirationAttributes(7);
      r1 = factory.setEntryTimeToLive(ea).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ea, ra.getEntryTimeToLive());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetCustomEntryTimeToLive() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      MyCustomExpiry ce = new MyCustomExpiry();
      r1 = factory.setCustomEntryTimeToLive(ce).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ce, ra.getCustomEntryTimeToLive());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetRegionIdleTimeout() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      ExpirationAttributes ea = new ExpirationAttributes(7);
      r1 = factory.setRegionIdleTimeout(ea).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ea, ra.getRegionIdleTimeout());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetRegionTimeToLive() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      ExpirationAttributes ea = new ExpirationAttributes(7);
      r1 = factory.setRegionTimeToLive(ea).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ea, ra.getRegionTimeToLive());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetKeyConstraint() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setKeyConstraint(String.class).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(String.class, ra.getKeyConstraint());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetValueConstraint() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setValueConstraint(String.class).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(String.class, ra.getValueConstraint());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetInitialCapacity() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setInitialCapacity(777).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(777, ra.getInitialCapacity());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetLoadFactor() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setLoadFactor(77.7f).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(77.7f, ra.getLoadFactor());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetConcurrencyLevel() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setConcurrencyLevel(7).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(7, ra.getConcurrencyLevel());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetDiskStoreName() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    c.createDiskStoreFactory().create("ds");
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
      r1 = factory.setDiskStoreName("ds").create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals("ds", ra.getDiskStoreName());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetDiskSynchronous() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL_PERSISTENT);
      r1 = factory.setDiskSynchronous(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.isDiskSynchronous());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetStatisticsEnabled() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setStatisticsEnabled(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getStatisticsEnabled());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetCloningEnabled() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(CACHING_PROXY);
      r1 = factory.setCloningEnabled(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getCloningEnabled());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetPoolName() throws CacheException, IOException
  {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(PROXY);
      r1 = factory.setPoolName("DEFAULT").create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals("DEFAULT", ra.getPoolName());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public static class MyCacheListener extends CacheListenerAdapter {
  }

  public static class MyCustomExpiry implements CustomExpiry {
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return null;
    }
    public void close() {
    }
  }
  
  public void testMultiUserRootRegions() throws Exception {
    Properties dsProps = new Properties();
    dsProps.setProperty("mcast-port", "0");
    DistributedSystem ds = DistributedSystem.connect(dsProps);
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 7777).setMultiuserAuthentication(true).create("muPool");
    PoolManager.createFactory().addServer(InetAddress.getLocalHost().getHostName(), 6666).create("suPool");
    ClientCache cc = new ClientCacheFactory()
      .create();
    cc.createClientRegionFactory(PROXY).setPoolName("muPool").create("p");
    cc.createClientRegionFactory(CACHING_PROXY).setPoolName("suPool").create("cp");
    cc.createClientRegionFactory(LOCAL).create("l");
    assertEquals(3, cc.rootRegions().size());

    {
      Properties muProps = new Properties();
      muProps.setProperty("user", "foo");
      RegionService rs = cc.createAuthenticatedView(muProps, "muPool");
      assertNotNull(rs.getRegion("p"));
      try {
        rs.getRegion("cp");
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      try {
        rs.getRegion("l");
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      assertEquals(1, rs.rootRegions().size());
      assertEquals(true, rs.getRegion("p") instanceof ProxyRegion);
      assertEquals(true, rs.rootRegions().iterator().next() instanceof ProxyRegion);
    }
  }

  /**
   * Make sure getLocalQueryService works.
   */
  public void testBug42294() throws Exception {
    ClientCache c = new ClientCacheFactory().create();
    QueryService qs = c.getLocalQueryService();
    ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
    Region r1 = factory.create("localRegion");
    Query q = qs.newQuery("SELECT * from /localRegion");
    q.execute();
  }
  
  public void testSubregionCreate() throws CacheException, IOException {
    ClientCache c = new ClientCacheFactory().create();
    Region r1 = null;
    Region sr1 = null;
    try {
      ClientRegionFactory factory = c.createClientRegionFactory(LOCAL);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(null, ra.getPoolName());
      
      sr1 = factory.createSubregion(r1, sr1Name);
      RegionAttributes sr1ra = sr1.getAttributes();
      assertEquals(DataPolicy.NORMAL, sr1ra.getDataPolicy());
      assertEquals(Scope.LOCAL, sr1ra.getScope());
      assertEquals(null, sr1ra.getPoolName());
      
      try {
        factory.createSubregion(r1, sr1Name);
        fail("Expected RegionExistsException");
      } catch (RegionExistsException expected) {
      }
      cleanUpRegion(sr1);
      cleanUpRegion(r1);
      try {
        factory.createSubregion(r1, sr1Name);
        fail("Expected RegionDestroyedException");
      } catch (RegionDestroyedException expected) {
      }
    } finally {
      cleanUpRegion(sr1);
      cleanUpRegion(r1);
    }
  }
}


