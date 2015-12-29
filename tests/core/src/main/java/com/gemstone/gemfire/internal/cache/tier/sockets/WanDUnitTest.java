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
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.cache.client.*;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

public class WanDUnitTest extends DistributedTestCase
{
  private static Cache cache = null;
  VM vm0 = null;
  VM vm1 = null;
  VM vm2 = null;
  VM vm3 = null;
  private static final String REGION_NAME = "WanDUnitTest_region";
  static WanDUnitTest impl;

  public static void createImpl() {
    impl = new WanDUnitTest("temp");
  }
  
  /** constructor */
  public WanDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    // use an instance to control inheritable aspects of the test
    vm0.invoke(getClass(), "createImpl", null);
    vm1.invoke(getClass(), "createImpl", null);
    vm2.invoke(getClass(), "createImpl", null);
    vm3.invoke(getClass(), "createImpl", null);
  }

  /**
   *tests what happens if a put is not succesful due the put being
   *blocked at the secondary server
   *
   */
  public void testOneServerNotResponding()
  {
    vm1.invoke(getClass(), "setWriter");
    Object ports1[] = (Object[])vm1.invoke(getClass(), "createServerCache1", new Object[]{null} /*no need of hub-port*/);
    vm2.invoke(getClass(), "setWriter");
    vm2.invoke(getClass(), "createServerCache2", ports1/* mcast-port, server-port*/);
    // vm3.invoke(getClass(), "createServerCache3");
    Object clientParams[] = new Object[] { getServerHostName(vm1.getHost()), ports1[0], ports1[1] };
    vm0.invoke(getClass(), "createClientCache", clientParams);
    // add expected exception on server due to interrupt of CacheWriter sleep
    final ExpectedException expectedEx = addExpectedException(
        "Unexpected Error on server", vm1);
    try {
      vm0.invoke(getClass(), "doPutsOnClientExpectingException");
    } finally {
      expectedEx.remove();
    }
  }

  public void testServerOverWanNotResponding()
  {
    Object ports3[] = (Object[])vm3.invoke(getClass(), "createServerCache3");    
    Object ports1[] = (Object[])vm1.invoke(getClass(), "createServerCache1",
        ports3 /*hub-port*/);
    vm2.invoke(getClass(), "createServerCache2", 
        ports1 /* mcast-port, server-port*/);

    vm3.invoke(getClass(), "setWriter");

    Object clientParams[] = new Object[] { getServerHostName(vm1.getHost()), ports1[0], ports1[1] };
    vm0.invoke(getClass(), "createClientCache", clientParams);
    // add expected exception on server due to interrupt of CacheWriter sleep
    final ExpectedException expectedEx = addExpectedException("interrupted",
        vm3);
    try {
      vm0.invoke(getClass(), "doPutsOnClientNotExpectingException");
    } finally {
      expectedEx.remove();
    }
  }

  /**
   * creates the cache
   * @param props
   * @return
   * @throws Exception
   */
  private Cache createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  /**
   * creates the client with retry 1
   * @throws Exception
   */
  public static void createClientCache(String host, Integer mcast_port, Integer server_port) throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = impl.createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory()
        .addServer(host, server_port.intValue())
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setMinConnections(2)
        .setSocketBufferSize(32768)
        .setReadTimeout(250)
        .setPingInterval(1000)
        .setRetryAttempts(1)
        .create("WanDUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  /**
   * create server 1
   * @throws Exception
   */
  public static Object createServerCache1(Object end_port ) throws Exception
  {
    Properties props = new Properties();
    int mcast_port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS) ;
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, Integer.toString(mcast_port));
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = impl.createCache(props);
    AttributesFactory factory = impl.getServerCacheAttributesFactory();
    factory.setEnableGateway(true);
    if (setWriter) {
      factory.setCacheWriter(new CacheWriterAdapter() {
        @Override
        public void beforeCreate(EntryEvent event) throws CacheWriterException
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }

        @Override
        public void beforeUpdate(EntryEvent event) throws CacheWriterException
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }
      });
      setWriter = false;
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int hub_port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    GatewayHub hub1 = cache.setGatewayHub("getwayhub1", hub_port);
    Gateway gateway1 = hub1.addGateway("gw1-" + mcast_port + hub_port  );

    if(end_port != null){
      Integer port = (Integer) end_port ;
      gateway1.addEndpoint("ep1", DistributedTestCase.getIPLiteral(), port.intValue());
    }else{
      gateway1.addEndpoint("ep1", DistributedTestCase.getIPLiteral(), AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    }
    GatewayQueueAttributes queueAttributes = gateway1.getQueueAttributes();
    queueAttributes.setMaximumQueueMemory(1);
    queueAttributes.setBatchSize(1);
    setDiskStoreForGateway(cache, gateway1.getId(), queueAttributes);  

    hub1.start();
    gateway1.start();
    BridgeServer server1 = cache.addBridgeServer();
    int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server1.setPort(serverPort);
    server1.setNotifyBySubscription(true);
    server1.start();
    Object ports[] = {new Integer(mcast_port),new Integer(server1.getPort())};
    return ports ;

  }

  /**
   * whether to set cache writer
   */
  static boolean setWriter = false;

  /**
   * create server 2 with writer
   *
   * @throws Exception
   */
  public static void createServerCache2(final Integer mcast_port, final Integer server_port ) throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, Integer.toString(mcast_port.intValue()));
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = impl.createCache(props);
    AttributesFactory factory = impl.getServerCacheAttributesFactory();
    factory.setEnableGateway(true);
    //set the writer which sleeps for a long time and hence
    //causing a timeout
    if (setWriter) {
      factory.setCacheWriter(new CacheWriterAdapter() {
        @Override
        public void beforeCreate(EntryEvent event) throws CacheWriterException
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }

        @Override
        public void beforeUpdate(EntryEvent event) throws CacheWriterException
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }
      });
      setWriter = false;
    }
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);
    if (region instanceof PartitionedRegion) {
      PRHARedundancyProvider.setForceLocalPrimaries(true);
      PartitionRegionHelper.assignBucketsToPartitions(region);
    }
    BridgeServer server1 = cache.addBridgeServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    server1.start();

  }

  public static Object createServerCache3() throws Exception
  {
    Properties props = new Properties();
    int mcast_port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS) ;
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, Integer.toString(mcast_port));
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    cache = impl.createCache(props);
    AttributesFactory factory = impl.getServerCacheAttributesFactory();
    factory.setEnableWAN(true);
    
    if (setWriter) {
      factory.setCacheWriter(new CacheWriterAdapter() {
        @Override
        public void beforeCreate(EntryEvent event) throws CacheWriterException
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }

        @Override
        public void beforeUpdate(EntryEvent event) throws CacheWriterException
        {
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }
      });
      setWriter = false;
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int hub_port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    GatewayHub hub1 = cache.setGatewayHub("getwayhub3", hub_port);
    Gateway gateway1 = hub1.addGateway("gw3-" +  mcast_port+ hub_port);
    GatewayQueueAttributes queueAttributes = gateway1.getQueueAttributes();
    queueAttributes.setMaximumQueueMemory(1);
    queueAttributes.setBatchSize(1);
    setDiskStoreForGateway(cache, gateway1.getId(), queueAttributes);
    hub1.start();
    gateway1.start();
    BridgeServer server1 = cache.addBridgeServer();
    int server_port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server1.setPort(server_port);
    server1.setNotifyBySubscription(true);
    server1.start();
    Object ports[] = {new Integer(hub_port)};
    return ports ;
  }

  protected AttributesFactory getServerCacheAttributesFactory()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory;
  }

  /**
   * close the clients and teh servers
   */
  @Override
  public void tearDown2() throws Exception
  {
    // unsetting the thread local variable so that
    // it doesn't affect other tests.
    vm0.invoke(getClass(), "unsetForceLocalPrimary");
    vm1.invoke(getClass(), "unsetForceLocalPrimary");
    vm2.invoke(getClass(), "unsetForceLocalPrimary");
    vm3.invoke(getClass(), "unsetForceLocalPrimary");
    // close the clients first
    vm0.invoke(getClass(), "closeCache");
    // then close the servers
    vm1.invoke(getClass(), "closeCache");
    vm2.invoke(getClass(), "closeCache");
    vm3.invoke(getClass(), "closeCache");
  }
  
  public static void unsetForceLocalPrimary() {
    if (cache != null && !cache.isClosed()) {
      Region region = cache.getRegion(REGION_NAME);
      if (region != null && region instanceof PartitionedRegion) {
        PRHARedundancyProvider.setForceLocalPrimaries(false);
      }
    }
  }

  /**
   * close the cache
   *
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void setWriter()
  {
    setWriter = true;
  }

  /**
   * does a put on the client expecting an exception
   * @throws Exception
   */
  public static void doPutsOnClientExpectingException() throws Exception
  {
    Region region1 = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
    try {
      region1.put("newKey", "newValue");
      fail("Exception did not occur although was supposed to occur");
    }
    catch (Exception e) {
      //e.printStackTrace();
    }
    assertEquals(null,region1.getEntry("newKey"));
  }


  /**
   * does a put on the client not expecting an exception
   * @throws Exception
   */
  public static void doPutsOnClientNotExpectingException() throws Exception
  {
    Region region1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    region1.put("newKey", "newValue");
    assertTrue("newValue".equals(region1.getEntry("newKey").getValue()));
  }
}
