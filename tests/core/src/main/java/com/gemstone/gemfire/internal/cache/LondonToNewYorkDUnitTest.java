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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.BridgeTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.TestCacheListener;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * This test creates a WAN gateway configuration between New York and London.
 * There are 4 VMs in this test arranged in this way:
 *
 *   NYC BridgeClient
 *
 *    <= ethernet =>
 *
 *   NYC Gateway/BridgeServer
 *
 *     < internet >
 *
 *   London Gateway/Bridgesrver
 *
 *   <= ethernet =>
 *
 *   London BridgeClient
 *
 *
 *  In addition this test has dynamic region create enabled.  The main purpose
 *  is to setup XML files for each VM (each has a separate Distrubted System etc.)
 *  load from the XML and create dynamic regions from NYC BridgeClient all the way through to the
 *  London BridgeClient
 *
 * @author Kirk Lund, Mitch Thomas
 * @since 4.3
 */
 public class LondonToNewYorkDUnitTest extends CacheTestCase {

   // These are terse because of lame windows constraints
  private static final String OVERFLOW_DIR = "_of";
  private static final String DISTRIBUTED_SYSTEM_PORT = "_DSPort";
  private static final String DYNAMIC_REGION_DIR = "_dynR";
  private static final String LONDON_CLIENT_NAME = "loCl";
  private static final String NYC_CLIENT_NAME = "nyCl";
  private static final String NYC_GATEWAY_NAME = "NYGW";
  private static final String LONDON_GATEWAY_NAME = "LoGW";

  protected static InternalLocator locator;


  private static final int BATCH_TIME_INTERVAL = 100; // assume ms

  private static final Integer CLIENT_RETRY = new Integer(250);

  public LondonToNewYorkDUnitTest(String name) {
    super(name);
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnectAllFromDS();
    cleanupAllLocators();
  }

  private void cleanupAllLocators() {
    final Host host = Host.getHost(0);
    for (int i = 0; i < host.getVMCount(); i++) {
      host.getVM(i).invoke(new SerializableRunnable("Stop locator") {
        public void run() {
          if (locator != null) {
            locator.stop();
            locator = null;
          }
        }
      });
    }
  }

  private VM setupLondonGateway(final String dsName, final int bsPort,
      final int gwHubPort,
      final String remoteHubHost, final int remoteHubPort, final int batchSize)
  throws Exception
  {
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);

    vm0.invoke(new CacheSerializableRunnable("Set up " + LONDON_GATEWAY_NAME) {
      public void run2() throws CacheException {
        createServerDS(dsName + "_" + LONDON_GATEWAY_NAME + "_vm0");

        beginCacheXml();
        if (getTestDynamicRegions()) {
          CacheCreation cc = (CacheCreation)getCache();
          File d = new File(dsName + DYNAMIC_REGION_DIR + hydra.ProcessMgr.getProcessId());
          d.mkdirs();
          cc.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(d, null));
        }

        createBridgeServer(bsPort);

        GatewayHub londonGatewayHub =
          getCache().setGatewayHub(dsName + "_" + LONDON_GATEWAY_NAME, gwHubPort);

        // add the london gateway to NYC
        Gateway gatewayToNYC = londonGatewayHub.addGateway(dsName + "_" + NYC_GATEWAY_NAME);

        gatewayToNYC.addEndpoint(
            dsName + "_" + NYC_GATEWAY_NAME + "Endpoint", remoteHubHost, remoteHubPort);
        // create the LondonGateway queue
        File d = new File(dsName + "_" + LONDON_GATEWAY_NAME + OVERFLOW_DIR + hydra.ProcessMgr.getProcessId());

        GatewayQueueAttributes LondonGatewayQueueAttributes =
          new GatewayQueueAttributes(d.toString(),
              GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
              batchSize,
              BATCH_TIME_INTERVAL,
              GatewayQueueAttributes.DEFAULT_BATCH_CONFLATION,
              GatewayQueueAttributes.DEFAULT_ENABLE_PERSISTENCE,
              GatewayQueueAttributes.DEFAULT_ENABLE_ROLLING,
              GatewayQueueAttributes.DEFAULT_ALERT_THRESHOLD);
        setDiskStoreForGateway(getCache(), d.toString(), LondonGatewayQueueAttributes);
        gatewayToNYC.setQueueAttributes(LondonGatewayQueueAttributes);

        createParentRegion(dsName, true);
        finishCacheXml(dsName + "_" + LONDON_GATEWAY_NAME);
      }
    });
    return vm0;
  }

  private VM setupLondonClient(final String dsName, final String bsHost, final int[] bsPorts) throws Exception {
    final Host host = Host.getHost(0);
    final VM vm1 = host.getVM(1);
    vm1.invoke(new CacheSerializableRunnable("Set up " + LONDON_CLIENT_NAME) {
      public void run2() throws CacheException {
        createLonerDS(dsName + "_loner_ " + LONDON_CLIENT_NAME + "_vm1");

        beginCacheXml();
        PoolImpl pi = null;
        if (getTestDynamicRegions()) {
          CacheCreation cc = (CacheCreation)getCache();
          pi = (PoolImpl)BridgeTestCase.configureConnectionPoolWithNameAndFactory(null, getServerHostName(host), bsPorts, true, -1, -1, null,"testPool",cc.createPoolFactory());
          File d = new File(dsName + DYNAMIC_REGION_DIR + hydra.ProcessMgr.getProcessId());
          d.mkdirs();
          cc.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(d, pi.getName(),false,true));
        } else {
          pi = (PoolImpl)BridgeTestCase.configureConnectionPool(null, getServerHostName(host), bsPorts, true, -1, -1, null);
        }
        createParentRegionOnClient(dsName, false, pi);
        finishCacheXml(dsName + "_" + LONDON_CLIENT_NAME);
        Region r = getCache().getRegion(dsName);
        r.registerInterestRegex(".*");
      }
    });
    return vm1;
  }

  private VM setupNYCGateway(final String dsName, final int bsPort, final int gwHubPort,
      final String remoteHubHost, final int remoteHubPort, final int batchSize) throws Exception {
    final Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);

    vm2.invoke(new CacheSerializableRunnable("Set up " + NYC_GATEWAY_NAME) {
      public void run2() throws CacheException {
        try {
//        getLogWriter().info("before createServerDS");
          createServerDS(dsName + "_" + NYC_GATEWAY_NAME + "_vm2");

          beginCacheXml();
          
          if (getTestDynamicRegions()) {
//            getLogWriter().info("before setdynamicRegionFactoryConfig");
            CacheCreation cc = (CacheCreation)getCache();
            File d = new File(dsName + DYNAMIC_REGION_DIR + hydra.ProcessMgr.getProcessId());
            d.mkdirs();
            cc.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(d, null));
          }

          createBridgeServer(bsPort);

//          getLogWriter().info("before getCache");
          Cache cache = getCache();
          // create the hub for LondonGateway (hub on LondonGateway side)

          GatewayHub londonClientHub =
            cache.setGatewayHub(dsName + "_" + NYC_GATEWAY_NAME, gwHubPort);
          // add the londonClient gateway (gateway to londonClient system)
          Gateway gatewayToLondon = londonClientHub.addGateway(dsName + "_" + LONDON_GATEWAY_NAME);
          // create the londonClient endpoint

          gatewayToLondon.addEndpoint(dsName + "_" + LONDON_GATEWAY_NAME + "Endpoint", remoteHubHost, remoteHubPort);
          // create the londonClient queue
          File gwdir = new File(dsName + "_" + NYC_GATEWAY_NAME + OVERFLOW_DIR + hydra.ProcessMgr.getProcessId());

          GatewayQueueAttributes londonClientQueueAttributes =
            new GatewayQueueAttributes(gwdir.toString(),
                GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
                batchSize,
                BATCH_TIME_INTERVAL,
                GatewayQueueAttributes.DEFAULT_BATCH_CONFLATION,
                GatewayQueueAttributes.DEFAULT_ENABLE_PERSISTENCE,
                GatewayQueueAttributes.DEFAULT_ENABLE_ROLLING,
                GatewayQueueAttributes.DEFAULT_ALERT_THRESHOLD);
          setDiskStoreForGateway(getCache(), gwdir.toString(), londonClientQueueAttributes);
          gatewayToLondon.setQueueAttributes(londonClientQueueAttributes);
          
//          getLogWriter().info("before createParentRegion");
          createParentRegion(dsName, true);
          
//          getLogWriter().info("before finishCacheXml");
          finishCacheXml(dsName + "_" + NYC_GATEWAY_NAME);
          
//          getLogWriter().info("all done");
        }
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable t) {
          throw new CacheException("Found throwable", t) {};
        }
      }
    });
    return vm2;
  }

  private VM setupNYCClient(final String dsName, final String bsHost, final int[] bsPorts) throws Exception {
    final Host host = Host.getHost(0);
    final VM vm3 = host.getVM(3);
    vm3.invoke(new CacheSerializableRunnable("Set up " + NYC_CLIENT_NAME) {
      public void run2() throws CacheException {
        createLonerDS(dsName + "_loner_" + NYC_CLIENT_NAME + "_vm3");
        beginCacheXml();
       
        PoolImpl pi = null;
        if (getTestDynamicRegions()) {
          CacheCreation cc = (CacheCreation)getCache();
          pi = (PoolImpl)BridgeTestCase.configureConnectionPoolWithNameAndFactory(null, getServerHostName(host), bsPorts, true, -1, -1, null,"testPool",cc.createPoolFactory());
          File d = new File(dsName + DYNAMIC_REGION_DIR + hydra.ProcessMgr.getProcessId());
          d.mkdirs();
          cc.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(d, pi.getName(),false,true));
        } else {
          pi = (PoolImpl)BridgeTestCase.configureConnectionPool(null, getServerHostName(host), bsPorts, true, -1, -1, null);
        }
        createParentRegionOnClient(dsName, false,pi);
        finishCacheXml(dsName + "_" + NYC_CLIENT_NAME);
        Region r = getCache().getRegion(dsName);
        r.registerInterestRegex(".*");
      }
    });
    return vm3;
  }


  //////////////////////  Test Methods  //////////////////////
  protected InternalDistributedSystem createLonerDS(String dsName) {
    disconnectFromDS();
    Properties lonerProps = new Properties();
    lonerProps.setProperty(DistributionConfig.NAME_NAME, dsName);
    lonerProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    lonerProps.setProperty(DistributionConfig.LOCATORS_NAME, "");
    InternalDistributedSystem ds = getSystem(lonerProps);
    assertEquals(0, ds.getDistributionManager().getOtherDistributionManagerIds().size());
    return ds;
  }

  protected InternalDistributedSystem createServerDS(String dsName) {
    int port = -1;
    try {
      File dsFile = new File(dsName + DISTRIBUTED_SYSTEM_PORT + ".obj");
      if (dsFile.exists()) {  // read it
        Integer i = (Integer) new ObjectInputStream(new FileInputStream(dsFile)).readObject();
        port = i.intValue();
      } else {  // create one and write it
        port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
        new ObjectOutputStream(new FileOutputStream(dsFile)).writeObject(new Integer(port));
      }
    }
    catch (ClassNotFoundException ignore) {
      // not possible to not cast from Integer
    }
    catch (IOException ioe) {
      fail("Problem reading or writing ds port", ioe);
    }
    final Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, Integer.toString(port));
    p.setProperty("mcast-ttl", "0");
    p.setProperty("locators", "");
    p.setProperty(DistributionConfig.NAME_NAME, dsName);
    InternalDistributedSystem ids = getSystem(p);
    assertEquals(0, ids.getDistributionManager().getOtherDistributionManagerIds().size());
    return ids;
  }

  protected Region createParentRegionOnClient(String name, boolean wanEnable, PoolImpl pool) throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEnableGateway(wanEnable);
    factory.setPoolName(pool.getName());
    factory.addCacheListener(new AtlanticTestCacheListener());
    final Region r = createRootRegion(name, factory.create());
    return r;
  }
  protected Region createParentRegion(String name, boolean wanEnable) throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    setParentRegionScope(factory);
    setStoragePolicy(factory);
    factory.setEnableGateway(wanEnable);
    factory.addCacheListener(new AtlanticTestCacheListener());
    final Region r = createRootRegion(name, factory.create());
    return r;
  }
  
  /** subclasses may reimplement this to use different storage models */
  void setStoragePolicy(AttributesFactory factory) {
    factory.setDataPolicy(DataPolicy.REPLICATE);
  }
  
  /** subclasses may reimplement this to use (or not) different scopes **/
  void setParentRegionScope(AttributesFactory factory) {
    factory.setScope(Scope.DISTRIBUTED_ACK);
  }

  /** whether dynamic regions should be tested (see LondondToNewYorkPRDUnitTest */
  boolean getTestDynamicRegions() {
    return true;
  }
  protected void createBridgeServer(int port) {
    Cache c = getCache();
    BridgeServer bridge = c.addBridgeServer();
    bridge.setNotifyBySubscription(true);
    bridge.setPort(port);
    try {
      bridge.start();
    } catch (IOException ioe) {
      fail("Could not start BridgeServer on port " + port);
    }
  }

  protected static void waitForGatewayInitialization(GatewayHubImpl ghi) {
    for (Iterator i = ghi.getGateways().iterator(); i.hasNext(); ) {
      final Gateway gi = (Gateway) i.next();
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return gi.isConnected();
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);
    }
  }

  protected static void waitForGatewayHubDistribution(GatewayHubImpl ghi, final int numEvents) {
    // Wait for hub to dispatch all the events events
    final GatewayHubStats ghs = ghi.getStatistics();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return ghs.getEventsProcessed() >= numEvents;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

    // Wait for the gateway queues to drain
    final List gws = ghi.getGateways();
    assertEquals(1, gws.size());
    for (Iterator gi = gws.iterator(); gi.hasNext(); ) {
      final Gateway gwi = (Gateway) gi.next();
      ev = new WaitCriterion() {
        public boolean done() {
          return gwi.getQueueSize() == 0;
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 10 * BATCH_TIME_INTERVAL, 200, true);
    }
  }

  protected void waitForUpdates(Region r, final int num) {
    // Wait until updates have arrived from London
    final AtlanticTestCacheListener atcl = (AtlanticTestCacheListener) r.getAttributes().getCacheListener();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return atcl.getMods() >= num;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
    atcl.resetMods();
  }
  
  public void testLondonToNewYork() throws Exception {
    // This is terse because of lame windows constraints (since its used for file and directory names)
    final String name = "LToNY" + Integer.toHexString(this.getClass().getName().hashCode());
    
    //getUniqueName();
    final Host host = Host.getHost(0);

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);

    final int londonGatewayBridgeServerPort = ports[0];
    final int nycGatewayBridgeSeverPort = ports[1];
    final int londonGatewayHubPort = ports[2];
    final int nycGatewayHubPort = ports[3];

    final int batchSize = 2;
    final VM nGateway = setupNYCGateway(name, nycGatewayBridgeSeverPort, nycGatewayHubPort, getServerHostName(host), londonGatewayHubPort, batchSize);
    final VM lGateway = setupLondonGateway(name, londonGatewayBridgeServerPort, londonGatewayHubPort, getServerHostName(nGateway.getHost()), nycGatewayHubPort, batchSize);

    final VM lClient = setupLondonClient(name, getServerHostName(lGateway.getHost()), new int[] {londonGatewayBridgeServerPort});
    final VM nClient = setupNYCClient(name, getServerHostName(nGateway.getHost()), new int[] {nycGatewayBridgeSeverPort});
    /*final String lKey = "londonKey";*/  final String lVal = "londonVal";
    final String dynKey = "dynKey";  final String dynVal = "dynVal";

    // Since London was the first up, wait for its gateways to NYC to get started
    lGateway.invoke(new CacheSerializableRunnable("Wait for gateway initialization") {
      public void run2() throws CacheException
      {
        List hubs = getCache().getGatewayHubs();
        GatewayHub hub = (GatewayHub)hubs.get(0);
        waitForGatewayInitialization((GatewayHubImpl) hub);
      }
    });

    lClient.invoke(new CacheSerializableRunnable("Test London client") {
      public void run2() throws CacheException
      {
        Region r = getRootRegion(name);
        assertNotNull(r);
        for(int i=batchSize; i>=0; i--) { // IMPORTANT, send one more than the batch size to kick the gateway
          Object k = new Key(i); // use non-String key to see if .* works
          r.put(k, lVal + i);
        }
      }
    });
    lGateway.invoke(new CacheSerializableRunnable("Test London gateway") {
      public void run2() throws CacheException
      {
        Region r = getRootRegion(name);
        assertNotNull(r);
        String v;
        Object k;
        for(int i=batchSize; i>=0; i--) {  // IMPORTANT, check that one more than the batch size arrived
          k = new Key(i);
          v = lVal + i;
          // if the region isn't partitioned, there should be an entry in this vm for the key
          if (r.getAttributes().getPartitionAttributes() == null) {
            assertNotNull("london key " + k, r.getEntry(k));
            assertEquals(v, v, r.getEntry(k).getValue());
          }
          else {
            assertEquals(v, v, r.get(k));
          }
        }
        List hubs = getCache().getGatewayHubs();
        GatewayHub hub = (GatewayHub)hubs.get(0);
        waitForGatewayHubDistribution((GatewayHubImpl) hub, batchSize);
      }
    });
    pause(2000); // waitForGatewayHubDistribution might depend on an async race
    nGateway.invoke(new CacheSerializableRunnable("Test NYC gateway") {
      public void run2() throws CacheException
      {
        Region r = getRootRegion(name);
        assertNotNull(r);
        String v;
        Object k;
        waitForUpdates(r, batchSize);

        for(int i=batchSize; i>0; i--) { // IMPORTANT only exepect the batch size to show up
          k = new Key(i);
          v = lVal + i;
//          if (r.getEntry(k) == null) {
//            getLogWriter().info("DEBUG Region r: " + r);
//            for(Iterator ri = r.entries(false).iterator(); ri.hasNext(); ){
//              Region.Entry re = (Region.Entry) ri.next();
//              getLogWriter().info("DEBUG  Entry " + re.getKey() + " -> " + re.getValue());
//            }
//          }
          if (r.getAttributes().getPartitionAttributes() == null) {
            assertNotNull("london key " + k, r.getEntry(k));
            assertEquals(v, v, r.getEntry(k).getValue());
          }
          else {
            assertEquals(v, v, r.get(k));
          }
        }
      }
    });
    nClient.invoke(new CacheSerializableRunnable("Test NYC client") {
      public void run2() throws CacheException
      {
        Region r = getRootRegion(name);
        assertNotNull(r);
        String v;
        Object k;
        for(int i=batchSize; i>batchSize; i--) {
          k = new Key(i);
          v = lVal + i;
          assertNotNull("london key " + k, r.getEntry(k));
          assertEquals(v, v, r.getEntry(k).getValue());
        }
      }
    });

    if (getTestDynamicRegions()) {
      // Verify that we can create a dynamic region in London client and have it propigate to NYC client
      final String dynamicRegionName = name + "-FromLondon";
      lClient.invoke(new CacheSerializableRunnable("Invoke dynamic region from London client") {
        public void run2() throws CacheException
        {
          Region dr = DynamicRegionFactory.get().createDynamicRegion(name, dynamicRegionName);
          assertNotNull(dr);
          dr.put(dynKey, dynVal);
        }
      });
      CacheSerializableRunnable dynRegionTest = new CacheSerializableRunnable("Test dynamic region on host") {
        public void run2() throws CacheException
        {
          Region r = getRootRegion(name);
          assertNotNull(r);
          Region dr = r.getSubregion(dynamicRegionName);
          assertNotNull(dr);
          assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynamicRegionName));
          assertNotNull("This assertion fails intermittently", dr.getEntry(dynKey));
          assertEquals(dynVal, dr.getEntry(dynKey).getValue());
        }
      };
      lGateway.invoke(dynRegionTest);
      lGateway.invoke(new CacheSerializableRunnable("Wait for Gateway distribution") {
        public void run2() throws CacheException
        {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          // two more updates: one for the dyn region creation, one for the dyn region update
          waitForGatewayHubDistribution((GatewayHubImpl)hub, 2 + batchSize);
        }
      });
      pause(2000); // waitForGatewayHubDistribution might depend on an async race
      nGateway.invoke(dynRegionTest); // this assert intermittently fail
      nClient.invoke(dynRegionTest);

      // Verify that disconnecting either of the clients and then re-connecting allows Dynamic Region to recover
      lClient.invoke(new CacheSerializableRunnable("Test cache reload on london Client") {
        public void run2() throws CacheException
        {
          closeCache(); // Close the cache and null out the static ref
          createLonerDS(name + "_loner_" + LONDON_CLIENT_NAME + "_vm1"); // closes existing ds, then creates new ds
          GemFireCacheImpl.testCacheXml =  new File(name +  "_" + LONDON_CLIENT_NAME + "-cache.xml");
          getCache();  // creates new cache with cache.xml
        }
      });
      lClient.invoke(dynRegionTest);
      nClient.invoke(new CacheSerializableRunnable("Test cache reload on nyc Client") {
        public void run2() throws CacheException
        {
          closeCache(); // Close the cache and null out the static ref
          createLonerDS(name + "_loner_" + NYC_CLIENT_NAME + "_vm3"); // closes existing ds, then reates new ds
          GemFireCacheImpl.testCacheXml =  new File(name + "_" + NYC_CLIENT_NAME + "-cache.xml");
          getCache(); // creates new cache with cache.xml
        }
      });
      nClient.invoke(dynRegionTest);

      // Tear down London and then build it back up again (using cache.xml), verify Dynamic Region recovery
      // since gateways have no GII features, we should not expect any data, but should expect the dyn Region
      lClient.invoke(CacheTestCase.class, "closeCache");
      lGateway.invoke(CacheTestCase.class, "closeCache");
      lGateway.invoke(new CacheSerializableRunnable("Test cache reload on london Gateway") {
        public void run2() throws CacheException
        {
          disconnectFromDS();
          createServerDS(name + "_" + LONDON_GATEWAY_NAME + "_vm0");
          GemFireCacheImpl.testCacheXml =  new File(name + "_" + LONDON_GATEWAY_NAME + "-cache.xml");
          getCache(); // creates new cache with existing cache.xml
        }
      });
      lClient.invoke(new CacheSerializableRunnable("Test cache reload on london Client again") {
        public void run2() throws CacheException
        {
          createLonerDS(name + "_loner_ " + LONDON_CLIENT_NAME + "_vm1"); // closes existing ds, then creates new ds
          GemFireCacheImpl.testCacheXml =  new File(name +  "_" + LONDON_CLIENT_NAME + "-cache.xml");
          getCache();  // creates new cache with existing cache.xml
        }
      });
      CacheSerializableRunnable confirmDynRegion = new CacheSerializableRunnable("Confirm dynamic region with no data on host") {
        public void run2() throws CacheException
        {
          Region r = getRootRegion(name);
          assertNotNull(r);
          assertEquals(AtlanticTestCacheListener.class, r.getAttributes().getCacheListener().getClass());
          Region dr = r.getSubregion(dynamicRegionName);
          assertNotNull(dr);
          assertEquals(AtlanticTestCacheListener.class, dr.getAttributes().getCacheListener().getClass());
          assertSame(r.getAttributes().getCacheListener(), dr.getAttributes().getCacheListener());
          assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynamicRegionName));
          assertNull(dr.getEntry(dynKey));
        }
      };
      lGateway.invoke(confirmDynRegion);
      lClient.invoke(confirmDynRegion);
    } // if (impl.getTestDynamicRegions())
    //Asif destroy dynamic regions at the end of the test
    CacheSerializableRunnable destroyDynRegn = new CacheSerializableRunnable("Destroy Dynamic regions") {
      public void run2() throws CacheException
      {
        Region dr = getCache().getRegion("__DynamicRegions");    
        if(dr != null) {
            dr.localDestroyRegion();      
        }
      }
    };
    lGateway.invoke(destroyDynRegn);
    nGateway.invoke(destroyDynRegn);
    lClient.invoke(destroyDynRegn);
    nClient.invoke(destroyDynRegn);
  }
  
  public static class AtlanticTestCacheListener extends TestCacheListener implements Declarable2 {
    private int numMods = 0;
    public synchronized void afterCreate2(EntryEvent event)  { this.numMods++; }
    public synchronized void afterUpdate2(EntryEvent event)  { this.numMods++; }
    public synchronized int getMods() {return this.numMods;}
    public Properties getConfig() { return new Properties(); }
    public void init(Properties props)  {}
    public synchronized void resetMods() {this.numMods = 0;}
  }
  
  static class Key implements java.io.Serializable {
    int myValue;
    
    Key(int value) {
      myValue = value;
    }
    
    public String toString() {
      return "TestKey" + myValue;
    }
    
    public int hashCode() {
      return toString().hashCode();
    }
    
    public boolean equals(Object other) {
      return toString().equals(other.toString());
    }
  }
}

