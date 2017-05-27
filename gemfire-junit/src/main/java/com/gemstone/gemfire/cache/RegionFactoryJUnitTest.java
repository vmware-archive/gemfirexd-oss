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

package com.gemstone.gemfire.cache;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static com.gemstone.gemfire.cache.RegionShortcut.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

/**
 * Unit test for the RegionFactory class
 * @author Mitch Thomas
 * @since 5.0
 */
public class RegionFactoryJUnitTest extends TestCase
{
  public static final String key = "key";
  public static final Integer val = new Integer(1);
  final String r1Name = "r1";
  final String r2Name = "r2";
  final String r3Name = "r3";
  final String r1sr1Name = "sr1";
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

  /*
   * Test method for 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory()'
   */
  public void testRegionFactory() throws CacheException
  {
    // Assert basic region creation when no DistributedSystem or Cache exists
    Region r1 = null, r2 = null, r1sr1 = null;
    try {
      RegionFactory factory = new RegionFactory();
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);

      // Assert duplicate creation failure
      try {
        factory.create(r1Name);
        fail("Expected RegionExistsException");
      }
      catch (RegionExistsException expected) {
      }
      
      r1sr1 = factory.createSubregion(r1, r1sr1Name);
      assertBasicRegionFunctionality(r1sr1, r1sr1Name);
      try {
        factory.createSubregion(r1, r1sr1Name);
        fail("Expected RegionExistsException");
      }
      catch (RegionExistsException expected) {
      }
      r1sr1.destroyRegion();

      r2 = factory.create(r2Name);
      assertBasicRegionFunctionality(r2, r2Name);
      r2.destroyRegion();
      
      try {
        factory.createSubregion(r2, "shouldNotBePossible");
        fail("Expected a RegionDestroyedException");
      } catch (RegionDestroyedException expected) {
      }

      // as of 6.5 if the cache that was used to create a regionFactory is closed
      // then the factory is out of business
      // @todo add a test to check this
//       // Assert we can handle a closed Cache
//       Cache c = r1.getCache();
      r1.destroyRegion();
//       c.close();
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);

      // as of 6.5 if the ds that was used to create a regionFactory is disconnected
      // then the factory is out of business
      // @todo add a test to check this
//       // Assert we can handle a disconnected disributed system
//       DistributedSystem d = r1.getCache().getDistributedSystem();
      r1.destroyRegion();
//       d.disconnect();
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);

      // as of 6.5 if the ds that was used to create a regionFactory is disconnected
      // then the factory is out of business
      // @todo add a test to check this
      // Assert we can handle both a closed Cache and a disconnected system
//       c = r1.getCache();
//       d = c.getDistributedSystem();
      r1.destroyRegion();
//       c.close();
//       d.disconnect();
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
    }
    finally {
      cleanUpRegion(r1sr1);
      cleanUpRegion(r1);
      cleanUpRegion(r2);
      try {
        tearDown();
      } catch (Exception tearDownBummer) {
        fail("Problem cleaning up: " + tearDownBummer);
      }
    }
    
    // Assert basic region creation when a Distributed system exists
    try {
      this.distSys = DistributedSystem.connect(new Properties()); // for teardown 

      RegionFactory factory = new RegionFactory();
      r1 = factory.create(r1Name);
      this.cache = r1.getCache(); // for teardown 
      assertBasicRegionFunctionality(r1, r1Name);
    }
    finally {
      cleanUpRegion(r1);
      try {
        tearDown();
      } catch (Exception tearDownBummer) {
        fail("Problem cleaning up: " + tearDownBummer);
      }
    }

    // Assert failure when a Distributed system exists but with different properties 
    try {
      this.distSys = DistributedSystem.connect(new Properties()); // for teardown 
      final Properties failed = new Properties();
      failed.put("mcast-ttl", "64");

      new RegionFactory(failed);
      fail("Expected exception");
    } catch (IllegalStateException expected) {
    }
    finally {
      cleanUpRegion(r1);
      try {
        tearDown();
      } catch (Exception tearDownBummer) {
        fail("Problem cleaning up: " + tearDownBummer);
      }
    }

    // Assert basic region creation when a Distributed and Cache exist
    try {
      DistributedSystem ds = DistributedSystem.connect(new Properties());
      CacheFactory.create(ds);

      RegionFactory factory = new RegionFactory();
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
    }
    finally {
      cleanUpRegion(r1);
      try {
        tearDown();
      } catch (Exception tearDownBummer) {
        fail("Problem cleaning up: " + tearDownBummer);
      }
    }
    
    // Assert basic region creation when a Distributed and Cache exist but the cache is closed
    try {
      DistributedSystem ds = DistributedSystem.connect(new Properties());
      this.cache = CacheFactory.create(ds);
      this.cache.close();

      RegionFactory factory = new RegionFactory();
      r1 = factory.create(r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
    }
    finally {
      cleanUpRegion(r1);
      try {
        tearDown();
      } catch (Exception tearDownBummer) {
        fail("Problem cleaning up: " + tearDownBummer);
      }
    }

  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(RegionAttributes)'
   */
  public void testRegionFactoryRegionAttributes() throws CacheException
  {
    Region r1 = null, r2 = null, r3 = null;
    try {
      Properties p = new Properties();
      p.put("mcast-port", "0");
    r1 = new RegionFactory(p).setScope(Scope.LOCAL)
        .setConcurrencyLevel(1).setLoadFactor(0.8F).setKeyConstraint(
            String.class).setStatisticsEnabled(true).create(r1Name);
    assertBasicRegionFunctionality(r1, r1Name);

    final RegionFactory factory = new RegionFactory(p, r1.getAttributes());
    r2 = factory.create(r2Name);
    assertBasicRegionFunctionality(r2, r2Name);
    assertEquals(r1.getAttributes(), r2.getAttributes());

    r3 = factory.create(r3Name);
    try {
      assertEquals(r2.getAttributes(), r3.getAttributes());
      fail("Expected r2 attributes to be different from r3");
    }
    catch (AssertionFailedError expected) {
    }
    } finally {
      cleanUpRegion(r1);
      cleanUpRegion(r2);
      cleanUpRegion(r3);
    }
  }
  

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(String)'
   */
  public void testRegionFactoryString() throws CacheException, IOException
  {

    DistributionConfig.DEFAULT_CACHE_XML_FILE.delete();
    Region r1 = null;
    try {
      DistributionConfig.DEFAULT_CACHE_XML_FILE.createNewFile();
      FileWriter f = new FileWriter(DistributionConfig.DEFAULT_CACHE_XML_FILE);
      f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
              + "<!DOCTYPE cache PUBLIC\n  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN\"\n"
              + "  \"http://www.gemstone.com/dtd/cache6_5.dtd\">\n"
              + "<cache>\n"
              + " <region-attributes id=\""
              + getName()
              + "\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n"
              + "  <key-constraint>"
              + String.class.getName()
              + "</key-constraint>\n"
              + "  <value-constraint>"
              + Integer.class.getName()
              + "</value-constraint>\n"
              + "    <entry-idle-time><expiration-attributes timeout=\"60\"/></entry-idle-time>\n"
              + " </region-attributes>\n" + "</cache>");
      f.close();

      RegionFactory factory = new RegionFactory(getName());
      r1 = factory.create(this.r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ra.getStatisticsEnabled(), true);
      assertEquals(ra.getScope().isDistributedAck(), true);
      assertEquals(ra.getValueConstraint(), Integer.class);
      assertEquals(ra.getKeyConstraint(), String.class);
      assertEquals(ra.getEntryIdleTimeout().getTimeout(), 60);
    }
    finally {
      DistributionConfig.DEFAULT_CACHE_XML_FILE.delete();
      cleanUpRegion(r1);
    }
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(Properties)'
   */
  public void testRegionFactoryProperties() throws CacheException, IOException
  {
    Region r1 = null;
    try {
      final Properties props1 = new Properties();
      props1.put("mcast-ttl", "64");
      RegionFactory factory = new RegionFactory(props1);
      r1 = factory.create(this.r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
      assertEquals(props1.get("mcast-ttl"), 
          r1.getCache().getDistributedSystem().getProperties().get("mcast-ttl"));
    } finally {
      cleanUpRegion(r1);
    }
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(Properties,
   * RegionAttributes)'
   */
  public void testRegionFactoryPropertiesRegionAttributes()
  {

  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.RegionFactory.RegionFactory(Properties,
   * String)'
   */
  public void testRegionFactoryPropertiesString() throws IOException, CacheException
  {
    Region r1 = null;
    File xmlFile = null;
    try {
      final Properties props2 = new Properties();
      props2.put("mcast-ttl", "64");
      final String xmlFileName = getName() + "-cache.xml";
      props2.put("cache-xml-file", xmlFileName);
      xmlFile = new File(xmlFileName);
      xmlFile.delete();
      xmlFile.createNewFile();
      FileWriter f = new FileWriter(xmlFile);
      final String attrsId = getName() + "-attrsId"; 
      f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
              + "<!DOCTYPE cache PUBLIC\n  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN\"\n"
              + "  \"http://www.gemstone.com/dtd/cache6_5.dtd\">\n"
              + "<cache>\n"
              + " <region-attributes id=\""
              + attrsId
              + "\" statistics-enabled=\"true\" scope=\"distributed-ack\">\n"
              + "  <key-constraint>"
              + String.class.getName()
              + "</key-constraint>\n"
              + "  <value-constraint>"
              + Integer.class.getName()
              + "</value-constraint>\n"
              + "    <entry-idle-time><expiration-attributes timeout=\"60\"/></entry-idle-time>\n"
              + " </region-attributes>\n" + "</cache>");
      f.close();

      RegionFactory factory = new RegionFactory(props2, attrsId);
      r1 = factory.create(this.r1Name);
      assertBasicRegionFunctionality(r1, r1Name);
      assertEquals(props2.get("mcast-ttl"), 
          r1.getCache().getDistributedSystem().getProperties().get("mcast-ttl"));
      assertEquals(props2.get("cache-xml-file"), 
          r1.getCache().getDistributedSystem().getProperties().get("cache-xml-file"));
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ra.getStatisticsEnabled(), true);
      assertEquals(ra.getScope().isDistributedAck(), true);
      assertEquals(ra.getValueConstraint(), Integer.class);
      assertEquals(ra.getKeyConstraint(), String.class);
      assertEquals(ra.getEntryIdleTimeout().getTimeout(), 60);
    } finally {
      if (xmlFile != null) {
        xmlFile.delete();
      }
      cleanUpRegion(r1);
    }

  }
  
  /**
   * Ensure that the RegionFactory set methods mirrors those found in RegionAttributes
   * 
   * @throws Exception
   */
  public void testAttributesFactoryConformance() throws Exception {
    Method[] af = AttributesFactory.class.getDeclaredMethods();
    Method[] rf = RegionFactory.class.getDeclaredMethods();
    Method am, rm;
    
    ArrayList afDeprected = new ArrayList(); // hack to ignore deprecated methods
    afDeprected.add("setCacheListener");
    afDeprected.add("setMirrorType");
    afDeprected.add("setPersistBackup");
    afDeprected.add("setBucketRegion");    
    afDeprected.add("setEnableWAN");    
    afDeprected.add("setEnableBridgeConflation");    
    afDeprected.add("setEnableConflation");    
    ArrayList methodsToImplement = new ArrayList();

    // Since the RegionFactory has an AttributesFactory member,
    // we only need to make sure the RegionFactory class adds proxies for the
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
      fail("RegionFactory does not conform to AttributesFactory, its should proxy these methods " + methodsToImplement);                
    }
  }

  private static Cache createCache() {
    Properties p = new Properties();
    p.setProperty("locators", "");
    return new CacheFactory(p)
      .set("mcast-port", "0")
      .create();
  }
  
  public void testPARTITION() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_REDUNDANT() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_PERSISTENT() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_PERSISTENT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_REDUNDANT_PERSISTENT() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_PERSISTENT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_OVERFLOW() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_REDUNDANT_OVERFLOW() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_PERSISTENT_OVERFLOW() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_PERSISTENT_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_REDUNDANT_PERSISTENT_OVERFLOW() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_PERSISTENT_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_HEAP_LRU() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_HEAP_LRU);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_REDUNDANT_HEAP_LRU() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT_HEAP_LRU);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testREPLICATE() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testREPLICATE_PERSISTENT() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PERSISTENT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testREPLICATE_OVERFLOW() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testREPLICATE_PERSISTENT_OVERFLOW() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PERSISTENT_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testREPLICATE_HEAP_LRU() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_HEAP_LRU);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
      assertEquals(new SubscriptionAttributes(InterestPolicy.ALL),
                   ra.getSubscriptionAttributes());
      assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_PERSISTENT() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_HEAP_LRU() throws CacheException, IOException
  {
    Cache c = new CacheFactory().create();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL_HEAP_LRU);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_OVERFLOW() throws CacheException, IOException
  {
    Cache c = new CacheFactory().create();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testLOCAL_PERSISTENT_OVERFLOW() throws CacheException, IOException
  {
    Cache c = new CacheFactory().create();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT_OVERFLOW);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
      assertEquals(Scope.LOCAL, ra.getScope());
      assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK), ra.getEvictionAttributes());
      assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
                   c.getResourceManager().getEvictionHeapPercentage());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_PROXY() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_PROXY);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testPARTITION_PROXY_REDUNDANT() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_PROXY_REDUNDANT);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
      assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testREPLICATE_PROXY() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
      assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetCacheLoader() throws CacheException, IOException {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
      CacheLoader cl = new MyCacheLoader();
      r1 = factory.setCacheLoader(cl).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(cl, ra.getCacheLoader());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetCacheWriter() throws CacheException, IOException {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
      CacheWriter cw = new MyCacheWriter();
      r1 = factory.setCacheWriter(cw).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(cw, ra.getCacheWriter());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testAddCacheListener() throws CacheException, IOException {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
      CacheListener cl = new MyCacheListener();
      r1 = factory.addCacheListener(cl).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(cl, ra.getCacheListener());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testInitCacheListener() throws CacheException, IOException {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE_PROXY);
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
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(77)).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(EvictionAttributes.createLRUEntryAttributes(77), ra.getEvictionAttributes());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetEntryIdleTimeout() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
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
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
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
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
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
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
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
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
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
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      ExpirationAttributes ea = new ExpirationAttributes(7);
      r1 = factory.setRegionTimeToLive(ea).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ea, ra.getRegionTimeToLive());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetScope() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setScope(Scope.GLOBAL).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(Scope.GLOBAL, ra.getScope());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetDataPolicy() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setDataPolicy(DataPolicy.REPLICATE).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetEarlyAck() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setEarlyAck(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getEarlyAck());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetMulticastEnabled() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setMulticastEnabled(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getMulticastEnabled());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetEnableGateway() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setEnableGateway(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getEnableGateway());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetEnableSubscriptionConflation() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setEnableSubscriptionConflation(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getEnableSubscriptionConflation());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetKeyConstraint() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setKeyConstraint(String.class).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(String.class, ra.getKeyConstraint());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetValueConstraint() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setValueConstraint(String.class).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(String.class, ra.getValueConstraint());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetInitialCapacity() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setInitialCapacity(777).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(777, ra.getInitialCapacity());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetLoadFactor() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setLoadFactor(77.7f).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(77.7f, ra.getLoadFactor());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetConcurrencyLevel() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setConcurrencyLevel(7).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(7, ra.getConcurrencyLevel());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetDiskStoreName() throws CacheException, IOException
  {
    Cache c = createCache();
    c.createDiskStoreFactory().create("ds");
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT);
      r1 = factory.setDiskStoreName("ds").create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals("ds", ra.getDiskStoreName());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetDiskSynchronous() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL_PERSISTENT);
      r1 = factory.setDiskSynchronous(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.isDiskSynchronous());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetPartitionAttributes() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory();
      PartitionAttributes pa = new PartitionAttributesFactory().setTotalNumBuckets(77).create();
      r1 = factory.setPartitionAttributes(pa).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(pa, ra.getPartitionAttributes());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetMembershipAttributes() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory();
      MembershipAttributes ma = new MembershipAttributes(new String[]{"role1", "role2"});
      r1 = factory.setMembershipAttributes(ma).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(ma, ra.getMembershipAttributes());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetIndexMaintenanceSynchronous() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE);
      r1 = factory.setIndexMaintenanceSynchronous(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getIndexMaintenanceSynchronous());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetStatisticsEnabled() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setStatisticsEnabled(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getStatisticsEnabled());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetIgnoreJTA() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE);
      r1 = factory.setIgnoreJTA(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getIgnoreJTA());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetLockGrantor() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE);
      r1 = factory.setScope(Scope.GLOBAL).setLockGrantor(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.isLockGrantor());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetSubscriptionAttributes() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(REPLICATE);
      SubscriptionAttributes sa = new SubscriptionAttributes(InterestPolicy.ALL);
      r1 = factory.setSubscriptionAttributes(sa).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(sa, ra.getSubscriptionAttributes());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetCloningEnabled() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setCloningEnabled(true).create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(true, ra.getCloningEnabled());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testSetGatewayHubId() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setGatewayHubId("hubId").create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals("hubId", ra.getGatewayHubId());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public void testSetPoolName() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    PoolManager.createFactory().addServer("127.0.0.1", 7777).create("myPool");
    try {
      RegionFactory factory = c.createRegionFactory(LOCAL);
      r1 = factory.setPoolName("myPool").create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals("myPool", ra.getPoolName());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testBug45749() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT);
      factory.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(5).create());
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(5, ra.getPartitionAttributes().getTotalNumBuckets());
      assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    } finally {
      cleanUpRegion(r1);
    }
  }
  public void testBug45749part2() throws CacheException, IOException
  {
    Cache c = createCache();
    Region r1 = null;
    try {
      RegionFactory factory = c.createRegionFactory(PARTITION_REDUNDANT);
      factory.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(5).setRedundantCopies(2).create());
      r1 = factory.create(this.r1Name);
      RegionAttributes ra = r1.getAttributes();
      assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
      assertNotNull(ra.getPartitionAttributes());
      assertEquals(5, ra.getPartitionAttributes().getTotalNumBuckets());
      assertEquals(2, ra.getPartitionAttributes().getRedundantCopies());
    } finally {
      cleanUpRegion(r1);
    }
  }

  public static class MyCacheListener extends CacheListenerAdapter {
  }
  public static class MyCacheLoader implements CacheLoader {
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }
    public void close() {
    }
  }
  public static class MyCacheWriter extends CacheWriterAdapter {
  }

  public static class MyCustomExpiry implements CustomExpiry {
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return null;
    }
    public void close() {
    }
  }
}


