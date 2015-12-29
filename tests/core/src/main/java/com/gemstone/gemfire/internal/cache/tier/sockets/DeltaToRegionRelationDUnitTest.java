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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import delta.DeltaTestObj;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * This tests the flag setting for region ( DataPolicy as Empty ) for
 * Delta propogation for a client while registering interest
 * 
 * @author aingle
 */
public class DeltaToRegionRelationDUnitTest extends DistributedTestCase {
  
  private static Cache cache = null;

  VM server = null;

  VM client = null;
 
  VM server2 = null;
  
  VM client2 = null;
 
  private static  int PORT1 ;
  
  private static  int PORT2 ;
  
/*  private static boolean isListenerCalled=false;*/
  /*
   * name of the region with data policy empty
   */
  private static final String REGION_NAME1 = "DeltaToRegionRelationDUnitTest_region1";
  
  /*
   * name of the region whose data policy is not empty
   */
  private static final String REGION_NAME2 = "DeltaToRegionRelationDUnitTest_region2";
  /*
   * to detect primary server
   */
  private static Integer primary = null;
  
  /** constructor */
  public DeltaToRegionRelationDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(1);
    server2 = host.getVM(2);
    client2 = host.getVM(3);
  }
  
  /**
   * This test does the following for single key Interest registration(<b>Regex</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple entries in map <br>
   */
  public void testDeltaToRegionForRegisterInterestRegex(){
    
    intialSetUp();
    
    // Register interest on region with data policy as EMPTY
    // key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerRegex",
        new Object[] {REGION_NAME1,  ".*" });
    
    // Register interest on region with data policy other then EMPTY
    // key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerRegex",
        new Object[] {REGION_NAME2,  ".*" });
    
    // validation on server side
    server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
   
    //  check for multiple time registration Interest
    
    //  Register interest on region with data policy as EMPTY
    //  key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerRegex",
        new Object[] {REGION_NAME1, ".8" });
    
    // Register interest on region with data policy as EMPTY
    // list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] { REGION_NAME1, new Integer(3), new Integer(4) });
    
    // Register interest on region with data policy other then EMPTY
    // list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerRegex",
        new Object[] { REGION_NAME2, ".8" });
    
    // validation on server side
    server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
    
    tearDownforSimpleCase();
  }
  
  /**
   * This test does the following for single key Interest registration(<b>RegisterInterestOp</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple entries in map <br>
   */
  public void testDeltaToRegionForRegisterInterest(){
    
    intialSetUp();
    
    // Register interest on region with data policy as EMPTY
    // key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] {REGION_NAME1,  new Integer(1) });
    
    // Register interest on region with data policy other then EMPTY
    // key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] {REGION_NAME2,  new Integer(1) });
    
    // validation on server side
    server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
   
    //  check for multiple time registration Interest
    
    //  Register interest on region with data policy as EMPTY
    //  key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] {REGION_NAME1,  new Integer(2) });
    
    // Register interest on region with data policy as EMPTY
    // list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] { REGION_NAME1, new Integer(3), new Integer(4) });
    
    // Register interest on region with data policy other then EMPTY
    // list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] { REGION_NAME2, new Integer(3), new Integer(4) });
    
    // validation on server side
    server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
    
    tearDownforSimpleCase();
  }
  
  /**
   * This test does the following for single key Interest registration(<b>RegisterInterestListOp</b>):<br>
   * 1)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 2)Verifies no create happens for region with data policy other then Empty; in map <br>
   * 3)Verifies multiple and different interest registration (key/list) should not create multiple entries in map <br>
   */
  public void testDeltaToRegionForRegisterInterestList(){
    
    intialSetUp();
    
    //  Register interest on region with data policy as EMPTY
    //  list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] {REGION_NAME1, new Integer(1), new Integer(2) });
    
    //  Register interest on region with data policy other then EMPTY
    //  list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] {REGION_NAME2, new Integer(1), new Integer(2) });
    
    // validation on server side
    server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
    
    // check for multiple time registration Interest
    
    //  Register interest on region with data policy as EMPTY
    //  list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] { REGION_NAME1, new Integer(3), new Integer(4) });
    
    //  Register interest on region with data policy as EMPTY
    //  key registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] { REGION_NAME1, new Integer(5) });
    
    //  Register interest on region with data policy as EMPTY
    //  list registration
    client.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] { REGION_NAME2, new Integer(3)});
    
    //  validation on server side
    server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
    
    tearDownforSimpleCase();
  }
  
  /**
   * This test does the following for single key Interest registration when <b>failover</b>(<b>RegisterInterestOp</b>):<br>
   * 1)Verifies when primary goes down, interest registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  public void testDeltaToRegionForRegisterInterestFailover(){
    
    intialSetUpForFailOver();
    // Register interest single key on region with data policy as EMPTY
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] {REGION_NAME1, new Integer(1) });
    // Register interest single key on region with data policy other then EMPTY
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeyOnly",
        new Object[] {REGION_NAME2, new Integer(1)});
        
    validationForFailOver();
    tearDownForFailOver();
  }
  
  /**
   * This test does the following for single key Interest registration when <b>failover</b>(<b>RegisterInterestListOp</b>):<br>
   * 1)Verifies when primary goes down, interest registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  public void testDeltaToRegionForRegisterInterestListFaliover() {
    intialSetUpForFailOver();
    // Register interest lists of keys on region with data policy as EMPTY
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] { REGION_NAME1, new Integer(1), new Integer(2) });

    // Register interest list of keys on region with data policy other then EMPTY
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "registerKeys",
        new Object[] { REGION_NAME2, new Integer(1), new Integer(2) });

    validationForFailOver();
    tearDownForFailOver();
  }
  
  /**
   * This test does the following for single key Interest registration when <b>failover</b>(<b>Regex</b>):<br>
   * 1)Verifies when primary goes down, interest registration happen <br>
   * 2)Verifies create entry for region with data policy as Empty; in map stored in CacheCleintProxy <br>
   * 3)Verifies no create happens for region with data policy other then Empty; in map <br>
   */
  public void testDeltaToRegionForRegisterInterestRegexFaliover() {
    intialSetUpForFailOver();
    // Register interest lists of keys on region with data policy as EMPTY
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "registerRegex",
        new Object[] { REGION_NAME1, ".9" });

    // Register interest list of keys on region with data policy other then EMPTY
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "registerRegex",
        new Object[] { REGION_NAME2, ".9" });

    validationForFailOver();
    tearDownForFailOver();
  }
  
  public static void validationOnServer() throws Exception {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);

    // wait
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return DeltaToRegionRelationDUnitTest.getClientProxy().isAlive()
            && DeltaToRegionRelationDUnitTest.getClientProxy()
                .getRegionsWithEmptyDataPolicy().containsKey(
                    Region.SEPARATOR + REGION_NAME1);
      }

      public String description() {
        return "Wait Expired";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 5 * 1000, 100, true);

    assertTrue("registerInterest not happened", proxy.hasRegisteredInterested());
    assertTrue(REGION_NAME1
        + " not present in cache client proxy : Delta is enable", proxy
        .getRegionsWithEmptyDataPolicy().containsKey(
            Region.SEPARATOR + REGION_NAME1)); /*
                                                 * Empty data policy
                                                 */
    assertFalse(REGION_NAME2
        + " present in cache client proxy : Delta is disable", proxy
        .getRegionsWithEmptyDataPolicy().containsKey(
            Region.SEPARATOR + REGION_NAME2)); /*
                                                 * other then Empty data policy
                                                 */
    assertTrue("Multiple entries for a region", proxy
        .getRegionsWithEmptyDataPolicy().size() == 1);
    assertTrue("Wrong ordinal stored for empty data policy", ((Integer)proxy
        .getRegionsWithEmptyDataPolicy().get(Region.SEPARATOR + REGION_NAME1))
        .intValue() == 0);

  }
  /*
   * create server cache
   */
  public static Integer createServerCache() throws Exception
  {
    new DeltaToRegionRelationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);
    cache.createRegion(REGION_NAME2, attrs);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());

  }
  /*
   * create client cache
   */
  public static void createClientCache(String host, Integer port)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationDUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    Region reg1 = cache.createRegion(REGION_NAME1, attrs);

    assertFalse(reg1.getAttributes().getCloningEnabled());
    factory.setDataPolicy(DataPolicy.NORMAL);
    attrs = factory.create();
    
    // region with non empty data policy
    Region reg2 = cache.createRegion(REGION_NAME2, attrs);
    assertFalse(reg2.getAttributes().getCloningEnabled());
    reg2.getAttributesMutator().setCloningEnabled(true);
    assertTrue(reg2.getAttributes().getCloningEnabled());
  }
  /*
   * create client cache and return's primary server location object (primary)
   */
  public static Integer createClientCache2(String host1, String host2,
      Integer port1, Integer port2) throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationDUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host1,
        port1.intValue()).addServer(host2, port2.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME1, attrs);

    factory.setDataPolicy(DataPolicy.NORMAL);
    attrs = factory.create();
    // region with non empty data policy
    cache.createRegion(REGION_NAME2, attrs);

    return new Integer(p.getPrimaryPort());
  }
  
  /*
   * create cache with properties
   */
  private  void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }
  
  public void tearDown2() throws Exception
  {
    super.tearDown2();
    cache = null;
    invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  /*
   * close cache
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  /*
   * get cache client proxy object
   */
  public static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor()
        .getCacheClientNotifier();
    
    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = (CacheClientProxy) i.next();
    }
    return proxy;
  }
  
  /*
   * get cache server / bridge server attacted to cache
   */
  private static BridgeServerImpl getBridgeServer() {
    BridgeServerImpl bridgeServer = (BridgeServerImpl)cache.getCacheServers()
        .iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }
  /*
   * number of client proxies are presert
   */
  private static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier()
        .getClientProxies().size();
  }
  /*
   * if expected number of proxies are not present and wait
   */
  private static void checkNumberOfClientProxies(int expected) {
    int current = getNumberOfClientProxies();
    int tries = 1;
    while (expected != current && tries++ < 60) {
      try {
        Thread.sleep(250);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt(); // reset bit
      }
      current = getNumberOfClientProxies();
    }
    assertEquals(expected, current);
  }
  /*
   * register single object on given region
   */
  private static void registerKeyOnly(String regionName, Object key) { //RegisterInterestOp

    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    r.registerInterest(key);
  }

  /*
   * register list of keys on given region
   */
  public static void registerKeys(String regionName, Integer obj1, Integer Obj2) //RegisterInterestListOp
  {
    List list = new ArrayList();
    try {
      Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      list.add(obj1);
      list.add(Obj2);
      r.registerInterest(list);
    }
    catch (Exception ex) {
      fail("failed while registering keys" + list + "", ex);
    }
  }

  /*
   * register regex on given region
   */
  public static void registerRegex(String regionName, String exp){
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    r.registerInterestRegex(exp);
  }
  
  /*
   * stop bridge server
   */
  public static void stopCacheServer(){
    getBridgeServer().stop();
  }
  /*
   * initial setup required for testcase with out failover
   */
  public void intialSetUp() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationDUnitTest.class,
        "createServerCache")).intValue();
    client
        .invoke(DeltaToRegionRelationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server.getHost()),
                new Integer(PORT1) });
  }
  /*
   * kind of teardown for testcase without failover
   */
  public void tearDownforSimpleCase() {
    //  close the clients first
    client.invoke(DeltaToRegionRelationDUnitTest.class, "closeCache");
    // then close the servers
    server.invoke(DeltaToRegionRelationDUnitTest.class, "closeCache");
  }  
  /*
   * initial setup required for testcase with failover
   */
  public void intialSetUpForFailOver() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationDUnitTest.class,
        "createServerCache")).intValue();
    // used only in failover tests
    PORT2 = ((Integer)server2.invoke(DeltaToRegionRelationDUnitTest.class,
        "createServerCache")).intValue();
    primary = (Integer)client2.invoke(
        DeltaToRegionRelationDUnitTest.class, "createClientCache2",
        new Object[] { getServerHostName(server.getHost()),
            getServerHostName(server2.getHost()), new Integer(PORT1),
            new Integer(PORT2) });
  }
  
  /*
   * find out primary and stop it
   * interest registeration whould happened on other server
   */
  public void validationForFailOver(){
    assertTrue(" primary server is not detected ",primary.intValue() != -1);
    if (primary.intValue() == PORT1) {
      server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
      server.invoke(DeltaToRegionRelationDUnitTest.class, "stopCacheServer");
      server2
          .invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
    }
    else {
      server2
          .invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
      server2.invoke(DeltaToRegionRelationDUnitTest.class, "stopCacheServer");
      server.invoke(DeltaToRegionRelationDUnitTest.class, "validationOnServer");
    }
  }
  /*
   * kind of teardown for testcase with failover
   */
  public void tearDownForFailOver() {
    // close the clients first
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "closeCache");
    // then close the servers
    server.invoke(DeltaToRegionRelationDUnitTest.class, "closeCache");
    server2.invoke(DeltaToRegionRelationDUnitTest.class, "closeCache");
  }
  
  
  public static void createClientCache3(String host, Integer port, Boolean bool)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DeltaToRegionRelationDUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setSocketBufferSize(32768)
        // .setRetryInterval(10000)
        // .setRetryAttempts(5)
        .create("DeltaToRegionRelationDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.EMPTY);
    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(REGION_NAME1, attrs);
    r.registerInterest("ALL_KEYS");
    assertFalse(cache.getRegion(REGION_NAME1).getAttributes()
        .getCloningEnabled());
  }

  public static void putDelta(String regName) {
    Region reg = cache.getRegion(Region.SEPARATOR + regName);
    DeltaTestObj obj = new DeltaTestObj();
    for (int i = 0; i < 10; i++) {
      obj.setIntVar(i);
      reg.put(new Integer(1), obj);
    }
  }

  public static void createDelta(String regName) {
    Region reg = cache.getRegion(Region.SEPARATOR + regName);
    DeltaTestObj obj = new DeltaTestObj();
    for (int i = 0; i < 10; i++) {
      obj.setIntVar(i);
      reg.create(new Integer(i), obj);
    }
  }

  
  public static void setTestFlagForAskFullValue(Boolean bool) {
    CacheClientUpdater.isUsedByTest = bool;
    DeltaTestObj.resetFromDeltaCounter();
  }

  public static void verifyNoDeltaPropagation() {
    assertFalse(CacheClientUpdater.fullValueRequested);
    assertFalse(DeltaTestObj.fromDeltaFeatureUsed());
    /*assertTrue(isListenerCalled);*/
  }
  
  public static void noDeltaFailureOnServer() {
    CachePerfStats stats = ((LocalRegion)cache.getRegion(REGION_NAME1))
        .getCachePerfStats();
    int deltaFailures = stats.getDeltaFailedUpdates();
    assertTrue("delta failures count is not zero", deltaFailures == 0);
    assertTrue("fromDelta invoked", !DeltaTestObj.fromDeltaFeatureUsed());
  }
  
  
  public static void waitForCreate(String r) {
    Region reg = cache.getRegion(REGION_NAME1);
    long elapsed = 0;
    long start = System.currentTimeMillis();
    while(elapsed < 10000 && reg.size() < 10){
      try {
        elapsed = System.currentTimeMillis() - start;
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    /*
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        Region reg = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
        int val = ((DeltaTestObj)reg.get(new Integer(1))).getIntVar();
        return val == 9;
      }

      public String description() {
        return "Updates NOT received.";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10 * 1000, 100, true);
*/    
  }
  
  /**
   * This test does the following 
   * 1)Verifies that full object request should not get called<br>
   * 2)Full object sent to cachelessclient<br>
   */
  public void testForEmptyDataPolicy() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationDUnitTest.class,
        "createServerCache")).intValue();
    client.invoke(DeltaToRegionRelationDUnitTest.class, "createClientCache3",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            new Boolean(false) });
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "createClientCache3",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            new Boolean(true) });
    client2.invoke(DeltaToRegionRelationDUnitTest.class,
        "setTestFlagForAskFullValue", new Object[] { new Boolean(true) });
    client.invoke(DeltaToRegionRelationDUnitTest.class, "putDelta",
        new Object[] { REGION_NAME1 });
    
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "waitForCreate",
        new Object[] { REGION_NAME1 });
    
    client2.invoke(DeltaToRegionRelationDUnitTest.class,
        "verifyNoDeltaPropagation");

    client2.invoke(DeltaToRegionRelationDUnitTest.class,
        "setTestFlagForAskFullValue", new Object[] { new Boolean(false) });
  }
  
  
  /**
   * This test does the following 
   * 1)Verifies that full object should be sent on wire for first create on key<br>
   */
  public void testForEmptyDataPolicyWithCreate() {
    PORT1 = ((Integer)server.invoke(DeltaToRegionRelationDUnitTest.class,
        "createServerCache")).intValue();
    client.invoke(DeltaToRegionRelationDUnitTest.class, "createClientCache3",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            new Boolean(false) });
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "createClientCache3",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            new Boolean(true) });
    /* clean flags */
    server.invoke(DeltaToRegionRelationDUnitTest.class,"resetFlags");
    client.invoke(DeltaToRegionRelationDUnitTest.class,"resetFlags");    
    client2.invoke(DeltaToRegionRelationDUnitTest.class,"resetFlags");
    
    client.invoke(DeltaToRegionRelationDUnitTest.class,
        "setTestFlagForAskFullValue", new Object[] { new Boolean(true) });
    client.invoke(DeltaToRegionRelationDUnitTest.class, "createDelta",
        new Object[] { REGION_NAME1 });
    
    client2.invoke(DeltaToRegionRelationDUnitTest.class, "waitForCreate",
        new Object[] { REGION_NAME1 });
    
    server.invoke(DeltaToRegionRelationDUnitTest.class, "noDeltaFailureOnServer");

    client.invoke(DeltaToRegionRelationDUnitTest.class,
        "setTestFlagForAskFullValue", new Object[] { new Boolean(false) });
  }
  
  public static void resetFlags(){
    DeltaTestObj.resetDeltaInvokationCounters();
  }
}
