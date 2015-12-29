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
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.VMCachedDeserializable;

import delta.DeltaTestObj;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * This tests for delta propagation in wan no delta propagation
 * 
 * @author aingle
 * @since 6.1
 */
public class DeltaPropgationWanValidationDUnitTest extends DistributedTestCase {

  private static final String REGION_NAME = "DeltaPropgationWanValidationDUnitTest_region";

  private static Cache cache = null;

  VM wan1 = null; // gatewayhub 1

  VM wan2 = null; // gatewayhub 2

  private static Integer ePort1;

  private static Integer ePort2;
  
  private static String key1="key1";
  
  private static String key2="key2";
  
  private static int lastUpdatedVal;

  static DeltaPropgationWanValidationDUnitTest impl;

  /** constructor */
  public DeltaPropgationWanValidationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    wan1 = host.getVM(0);
    wan2 = host.getVM(1);
    // use an instance to control inheritable aspects of the test
    wan1.invoke(getClass(), "createImpl", null);
    wan2.invoke(getClass(), "createImpl", null);
  }

  public static void createImpl() {
    impl = new DeltaPropgationWanValidationDUnitTest("temp");
  }
  
  /**
   * This test does the following :<br>
   * 1)Create two wan site <br>
   * 2)Perform put operation with Delta types as object <br>
   * 3)Verifies full object is recieved<br>
   */
  public void testDeltaPropgationValidationWan() {
    ePort2 = new Integer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    ePort1 = new Integer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    
    wan1.invoke(getClass(), "createServerCache",
        new Object[] {"EU", "1", ePort1, ePort2, getServerHostName(wan1.getHost()) });
    
    wan2.invoke(getClass(), "createServerCache",
        new Object[] {"NY", "2", ePort2, ePort1, getServerHostName(wan2.getHost())});
    
    try {
      Thread.sleep(10000);

      wan1.invoke(getClass(), "doPuts", new Object[] {key1});

      wan2.invoke(getClass(), "doPuts", new Object[] {key2}); 
      
      // wait for some time so that event processed
      Thread.sleep(10000);
    }
    catch (InterruptedException e) {
      fail("interrupted");
    }
    
//  perfrom get operation
    wan2.invoke(getClass(), "doGet",
        new Object[] { new Integer(2) /* only 2 keys created */});
    wan1.invoke(getClass(), "doGet",
        new Object[] { new Integer(2) /* only 2 keys created */});
  }

  /**
   * Creates the server cache, and configure wan
   * 
   * @param site -
   *          used to configure gateway endpoint
   * @param name -
   *          used to generate name
   * @param ePort1 -
   *          GatewayHub port
   * @param ePort2 -
   *          other GatewayHub port
   * @throws Exception -
   *           thrown if any problem occurs in setting up the server
   */
  public static Object createServerCache(String site, String name,
      Integer ePort1, Integer ePort2, String host) throws Exception {
    Properties props = new Properties();
    int mcast_port = AvailablePort
        .getRandomAvailablePort(AvailablePort.JGROUPS);
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, mcast_port + "");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = impl.createCache(props);
    AttributesFactory factory = impl.getServerCacheAttributesFactory();
    factory.setEnableGateway(true);

    factory.setCacheWriter(new CacheWriterAdapter() {
      public void beforeCreate(EntryEvent event) throws CacheWriterException {
        cache.getLogger().info("event before create" + event.toString());
      }

      public void beforeUpdate(EntryEvent event) throws CacheWriterException {
        cache.getLogger().info(
            "event before update " + ((DeltaTestObj)(event.getNewValue())));
        assertEquals(((DeltaTestObj)event.getNewValue()).getFromDeltaCounter(),
            0);
      }
    });
    // clonning disable
    factory.setCloningEnabled(false);
    factory.setConcurrencyChecksEnabled(false);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    GatewayHub hub1 = cache.addGatewayHub("GatewayHub" + name, ePort1
        .intValue());
    Gateway gateway1 = hub1.addGateway("Gateway" + name + ePort1);
    gateway1.addEndpoint(site, host, ePort2.intValue());
    cache.getLogger().info("site :: " + site + "endPoint port:::" + ePort2);
    
    GatewayQueueAttributes queueAttributes = gateway1.getQueueAttributes();
    queueAttributes.setMaximumQueueMemory(1);
    queueAttributes.setBatchSize(1);
    setDiskStoreForGateway(cache, gateway1.getId(), queueAttributes);
    hub1.start();
    gateway1.start();
    return new Integer(0);
  }

  protected AttributesFactory getServerCacheAttributesFactory() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory;
  }

  /**
   * creates the cache
   * 
   * @param props
   * @return Cache
   * @throws Exception
   */
  private Cache createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * close the clients and the servers
   */
  public void tearDown2() throws Exception {
    /* close the cache */
    wan1.invoke(getClass(), "closeCache");
    wan2.invoke(getClass(), "closeCache");
    cache = null;
    invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  /**
   * close the cache
   * 
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  /**
   * perform put operation
   *
   */
  public static void doPuts(String key) throws Exception
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    DeltaTestObj obj = new DeltaTestObj();
    // first create
    region1.put(key, obj);
    
    for(int i=1; i<200; i++){
      obj.setIntVar(i);
      cache.getLogger().info("put happened value : " +obj);
      region1.put(key, obj);
      lastUpdatedVal=i;
    }
    cache.getLogger().info("put happened for key : " +key);
  }

  /**
   * perform get operation to verify total entry count recieved
   *
   */
  public static void doGet(final Integer n) throws Exception {
    final Region reg = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int curSize = reg.keySet().size();
        if (n.intValue() == curSize) {
          assertTrue(((DeltaTestObj)reg.get(key1)).getIntVar() == lastUpdatedVal);
          assertTrue(((DeltaTestObj)reg.get(key2)).getIntVar() == lastUpdatedVal);
          return true;
        }
        excuse = "Current size " + curSize + " not expected size " + n.intValue();
        return false;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120 * 1000, 1000, true);
  }
  
}
