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
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * This tests for delta propagation in wan no delta propagation
 * 
 * @author aingle
 * @since 6.1
 */
public class WanTXValidationDUnitTest extends DistributedTestCase {

  private static final String REGION_NAME = "WanTXValidationDUnitTest_region";

  private static Cache cache = null;

  VM accessor1 = null; // gatewayhub 1

  VM accessor2 = null; // gatewayhub 2
  
  VM datastore1 = null; // gatewayhub 1
  
  VM datastore2 = null; // gatewayhub 1
  

//  private static Integer ePort1;
//
//  private static Integer ePort2;
  
  private static String key1="key1";
  
  private static String key2="key2";
  
  private static int lastUpdatedVal = 199;

  static WanTXValidationDUnitTest impl;

  /** constructor */
  public WanTXValidationDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    datastore1 = host.getVM(0);
    datastore2 = host.getVM(1);
    accessor1 = host.getVM(2);
    accessor2 = host.getVM(3);
    
  }

  public static void createImpl() {
    impl = new WanTXValidationDUnitTest("temp");
  }
  
  /**
   * This test does the following :<br>
   * 1)Create two wan site <br>
   * 2)Perform put operation with Delta types as object <br>
   * 3)Verifies full object is recieved<br>
   */
  public void testPutAndCommitInTheDatastoreWithGatewayInDS() {
    doBasicTest(datastore1,datastore2,null,null,datastore1,datastore2,true);
  }
  
  public void DISABLED_TILL_NEW_TX_IMPL_COMPLETE_testPutAndCommitInTheDatastoreWithGatewayInAccessor() {
    doBasicTest(accessor1,accessor2,datastore1,datastore2,datastore1,datastore2,false);
  }
  
  
  public void DISABLED_TILL_NEW_TX_IMPL_COMPLETE_testPutAndCommitInTheAccessorWithGatewayInAccessor() {
    doBasicTest(accessor1,accessor2,datastore1,datastore2,accessor1,accessor2,false);
  }
  
  
  public void testPutAndCommitInTheAccessorWithGatewayInDatastore() {
    doBasicTest(datastore1,datastore2,accessor1,accessor2,accessor1,accessor2,true);
  }
  
  
  public void doBasicTest(VM gnode1,VM gnode2,VM anode1,VM anode2,VM doPuts1,VM doPuts2, boolean storageInGateway) {
 // use an instance to control inheritable aspects of the test
    gnode1.invoke(getClass(), "createImpl", null);
    gnode2.invoke(getClass(), "createImpl", null);
    if(anode1!=null) {
      anode1.invoke(getClass(), "createImpl", null);
      anode2.invoke(getClass(), "createImpl", null);
    }
      
    int ePort2 = new Integer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    int ePort1 = new Integer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    
    int mcast_port1 = AvailablePort
    .getRandomAvailablePort(AvailablePort.JGROUPS);
    int mcast_port2 = AvailablePort
    .getRandomAvailablePort(AvailablePort.JGROUPS);
    
    
    gnode1.invoke(getClass(), "createServerCache",
        new Object[] {"EU", "NY", ePort1, ePort2, getServerHostName(gnode1.getHost()),mcast_port1,true,storageInGateway });
    
    if(anode1!=null) {
      anode1.invoke(getClass(), "createServerCache",
          new Object[] {"EU", "NY", ePort1, ePort2, getServerHostName(anode1.getHost()),mcast_port1,false,!storageInGateway});
    }
    
    
    
    
    gnode2.invoke(getClass(), "createServerCache",
        new Object[] {"NY", "EU", ePort2, ePort1, getServerHostName(gnode2.getHost()),mcast_port2,true,storageInGateway});
    
    
    if(anode2!=null) {
      anode2.invoke(getClass(), "createServerCache",
          new Object[] {"NY", "EU", ePort2, ePort1, getServerHostName(anode2.getHost()),mcast_port2,false,!storageInGateway});
    }
    
    
    try {
      Thread.sleep(10000);

      doPuts1.invoke(getClass(), "doPuts", new Object[] {key1});
      doPuts2.invoke(getClass(), "doPuts", new Object[] {key2}); 
      
      // wait for some time so that event processed
      Thread.sleep(10000);
    }
    catch (InterruptedException e) {
      fail("interrupted");
    }
    
//  perfrom get operation
    gnode2.invoke(getClass(), "doGet",
        new Object[] { new Integer(2) /* only 2 keys created */});
    gnode1.invoke(getClass(), "doGet",
        new Object[] { new Integer(2) /* only 2 keys created */});
    
    if(anode1!=null) {
      anode2.invoke(getClass(), "doGet",
          new Object[] { new Integer(2) /* only 2 keys created */});
      
      anode1.invoke(getClass(), "doGet",
          new Object[] { new Integer(2) /* only 2 keys created */});
    }
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
  public static Object createServerCache(String myHubId, String hubImSendingTo,
      Integer listenOnPort, Integer sendToPort, String host,int mcast_port,boolean enableStorage,boolean enableGateway) throws Exception {
    Properties props = new Properties();
    
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, mcast_port + "");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = impl.createCache(props);
    AttributesFactory factory = impl.getServerCacheAttributesFactory(enableStorage);
    factory.setEnableGateway(true);
    factory.setCloningEnabled(false);
    factory.setCacheListener(new CacheListener() {

      public void afterCreate(EntryEvent event) {
        // TODO Auto-generated method stub
      }

      public void afterDestroy(EntryEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterInvalidate(EntryEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterRegionClear(RegionEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterRegionCreate(RegionEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterRegionDestroy(RegionEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterRegionInvalidate(RegionEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterRegionLive(RegionEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void afterUpdate(EntryEvent event) {
        // TODO Auto-generated method stub
        
      }

      public void close() {
        // TODO Auto-generated method stub
        
      }
    });

    RegionAttributes attrs = factory.create();
    
    cache.createRegion(REGION_NAME, attrs);
    
    if(enableGateway) {
      GatewayHub hub1 = cache.addGatewayHub(myHubId, listenOnPort
          .intValue());
      Gateway gateway1 = hub1.addGateway(hubImSendingTo);
      gateway1.addEndpoint("dont matter", host, sendToPort.intValue());
      
      GatewayQueueAttributes queueAttributes = gateway1.getQueueAttributes();
      queueAttributes.setMaximumQueueMemory(1);
      queueAttributes.setBatchSize(1);
      setDiskStoreForGateway(cache, gateway1.getId(), queueAttributes);
      hub1.start();
      gateway1.start();
      
    }
    return new Integer(0);
  }

  protected AttributesFactory getServerCacheAttributesFactory(boolean enableStorage) {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    factory.setDataPolicy(DataPolicy.PARTITION);
    paf.setRedundantCopies(0).setTotalNumBuckets(1);
    if(!enableStorage) {
      paf.setLocalMaxMemory(0);
    }

    factory.setPartitionAttributes(paf.create());
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
  @Override
  public void tearDown2() throws Exception {
    /* close the cache */
    datastore1.invoke(getClass(), "closeCache");
    datastore2.invoke(getClass(), "closeCache");
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
    // first create
    
    for(int i=1; i<200; i++){
      Object obj = "sup"+i;
      cache.getLogger().info("put happened value : " +obj);
      region1.getCache().getCacheTransactionManager().begin();
      region1.put(key, obj);
      region1.getCache().getCacheTransactionManager().commit();
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
          assertTrue("was supposed to be:"+lastUpdatedVal+" but was "+((String)reg.get(key1)).substring(3),((String)reg.get(key1)).substring(3).equals(""+lastUpdatedVal));
          assertTrue("was supposed to be:"+lastUpdatedVal+" but was "+((String)reg.get(key2)).substring(3),((String)reg.get(key2)).substring(3).equals(""+lastUpdatedVal));
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
