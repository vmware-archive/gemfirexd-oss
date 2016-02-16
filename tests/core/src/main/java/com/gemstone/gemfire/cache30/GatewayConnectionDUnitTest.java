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
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GatewayEventCallbackArgument;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientHealthMonitor;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;
import hydra.ProcessMgr;

/**
 * Tests the WAN Gateway functionality.
 *
 * @author agingade
 * @since 4.3
 */
public class GatewayConnectionDUnitTest extends CacheTestCase {
  
  // TODO: use these in Congo with static setUp and tearDown
//  private static final String expected = "java.net.ConnectException";
//  private static final String addExpected =
//    "<ExpectedException action=add>" + expected + "</ExpectedException>";
//  private static final String removeExpected =
//    "<ExpectedException action=remove>" + expected + "</ExpectedException>";
  
  private static final String WAN_REGION_NAME = "GatewayDUnitTest_WAN_Root";
  private static final String DS_REGION_NAME = "GatewayDUnitTest_DS_Root";
  
  protected static InternalLocator locator;
  
  private static final int NUM_KEYS = 10;
  
  private VM vm0 = null;
  private VM vm1 = null;
  private VM vm2 = null;
  private VM vm3 = null;
  
  private VM[] vmsDS0 = new VM[0];
  private VM[] vmsDS1 = new VM[0];

  private String[] vmsNameDS0 = null;
  private String[] vmsNameDS1 = null;

  public GatewayConnectionDUnitTest(String name) {
    super(name);
  }
  
  // this test has special config of distributed systems so
  // the setUp and tearDown methods need to make sure we don't
  // use the ds from previous test and that we don't leave ours around
  // for the next test to use.
  
  @Override
  public void setUp() throws Exception {
    boolean failedSetUp = true;
    try {
      disconnectAllFromDS();
      setUpSystems();
      failedSetUp = false;
    } finally {
      super.setUp();
      if (failedSetUp) {
        disconnectAllFromDS();
        cleanupAllLocators();
      }
    }
  }
  
  @Override
  public void tearDown2() throws Exception {
    getLogWriter().info("tearDown()");
    try {
      closeAllCache();
    }
    finally {
      disconnectAllFromDS();
      cleanupAllLocators();
    }
  }
  
  private void cleanupAllLocators() {
    getLogWriter().info("cleanupAllLocators()");

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
  
  /**
   * Sets up two distributed systems and gateways.
   * <p>
   * VM0 and VM1 will form DS0<br>
   * VM2 and VM3 will form DS1
   * <p>
   * DS0 and DS1 are two separate distributed systems<br>
   * VM0 and VM2 will be standard system members
   * VM1 and VM3 will act as Gateways
   * <p>
   * Controller VM will simply run the test and not participate.
   */
  private void setUpSystems() throws Exception {
    getLogWriter().info("SetUpSystems()");

    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    
    vmsDS0 = new VM[] { vm0, vm1 };
    vmsDS1 = new VM[] { vm2, vm3 };

    vmsNameDS0 = new String[] {"vm0", "vm1"};
    vmsNameDS1 = new String[] {"vm2", "vm3"};

    String hostName = getServerHostName(host);
    
    int[] freeUDPPorts = AvailablePortHelper.getRandomAvailableUDPPorts(2);
    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int dsPortDS0 = freeUDPPorts[0];
    int dsPortDS1 = freeUDPPorts[1];
    int hubPortDS0 = freeTCPPorts[0];
    int hubPortDS1 = freeTCPPorts[1];
    
    setUpDS("ds0", dsPortDS0, vmsDS0, hubPortDS0,
        "ds1", hostName, hubPortDS1, vmsNameDS0);
    setUpDS("ds1", dsPortDS1, vmsDS1, hubPortDS1,
        "ds0", hostName, hubPortDS0, vmsNameDS1);
  }
  
  private void setUpDS(final String dsName,
      final int dsPort,
      final VM[] vms,
      final int hubPortLocal,
      final String dsNameRemote,
      final String hostNameRemote,
      final int hubPortRemote,
      final String[] vmsName)
  throws Exception {
    getLogWriter().info("SetUpDS()");

    // setup DS
    final Properties propsDS = new Properties();
    propsDS.setProperty("mcast-port", String.valueOf(dsPort));
    propsDS.setProperty("locators", "");
    
    // connect to DS in both vms
    for (int i = 0; i < vms.length; i++) {
      final int whichvm = i;
      final VM vm = vms[whichvm];
      vm.invoke(new CacheSerializableRunnable("Set up "+dsName) {
        @Override
        public void run2() throws CacheException {
          String vmName = "GatewayConnectionDUnitTest_" + dsName + vmsName[whichvm];
          propsDS.setProperty(DistributionConfig.NAME_NAME, vmName);
          getSystem(propsDS);
          getLogWriter().info("[GatewayConnectionDUnitTest] " + vmName + " has joined " +
              dsName + " with port " + String.valueOf(dsPort));
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          
          // create WAN region
          factory.setEnableWAN(true);
          factory.setCacheListener(new EventListener());
          createRegion(WAN_REGION_NAME,
              factory.create());
          
          // create DS region
          factory.setEnableWAN(false);
          factory.setCacheListener(new EventListener());
          createRegion(DS_REGION_NAME,
              factory.create());
          getLogWriter().info("[GatewayConnectionDUnitTest] " + vmName + " has created both regions");
        }
      });
    }
    
    // set up gateway in last vm
    final int whichvm = vms.length-1;
    final VM vm = vms[whichvm];
    
    vm.invoke(new CacheSerializableRunnable("Set up gateway in "+dsName) {
      @Override
      public void run2() throws CacheException {
        String vmName = "GatewayConnectionDUnitTest_" + dsName + vmsName[whichvm];
        
        // create the gateway hub
        String hubName = "GatewayConnectionDUnitTest_"+dsName;
        String gatewayName = "GatewayConnectionDUnitTest_"+dsNameRemote;
        getLogWriter().info("[GatewayConnectionDUnitTest] " + vmName + " is creating " +
            hubName + " with gateway to " + gatewayName);

        Cache cache = getCache();
        GatewayHub hub = cache.setGatewayHub(hubName, hubPortLocal);
        Gateway gateway = hub.addGateway(gatewayName);
        
        // create the endpoint for remote DS
        getLogWriter().info("[GatewayConnectionDUnitTest_] " + vmName +
            " adding endpoint [" + gatewayName + ", " + hostNameRemote + ", " +
            hubPortRemote + "] to " + gatewayName);
        gateway.addEndpoint(gatewayName, hostNameRemote, hubPortRemote);
        
        // create the gateway queue
        File d = new File(gatewayName + "_overflow_" + ProcessMgr.getProcessId());
        getLogWriter().info("[GatewayConnectionDUnitTest_] " + vmName +
            " creating queue in " + d + " for " + gatewayName);
        
        GatewayQueueAttributes queueAttributes =
          new GatewayQueueAttributes(d.toString(),
              GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
              1, //GatewayQueueAttributes.DEFAULT_BATCH_SIZE,
              GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL,
              GatewayQueueAttributes.DEFAULT_BATCH_CONFLATION,
              GatewayQueueAttributes.DEFAULT_ENABLE_PERSISTENCE,
              GatewayQueueAttributes.DEFAULT_ENABLE_ROLLING,
              GatewayQueueAttributes.DEFAULT_ALERT_THRESHOLD);
        gateway.setQueueAttributes(queueAttributes);
        try {
          hub.start();
        }
        catch (IOException e) {
          getLogWriter().error("Start of hub " + hubName + " threw IOException", e);
          fail("Start of hub " + hubName + " threw IOException");
        }
        getLogWriter().info("[GatewayConnectionDUnitTest] " + vmName + " has created " +
            hubName + " with gateway to " + gatewayName);
      }
    });
    
  }
  
  //////////////////////  Test Methods  //////////////////////
  
  /**
   * This calls many sub-tests since setUp and tearDown are so complex.
   * Gateway funcationality is tested.
   */
  public void testGateway() throws Exception {
    doTestGatewayReConnect();
  }
  
  
  /** 
   * This tests the reconnected functionality of the Gateway client.
   * Test is added to test the bug related to missing batch-id error(bugid 35593).
   * Scenario:
   *    Sender Gateway updates the Wan Region.
   *    While receiving updates; receiver disonnects with all the Gateway clients, 
   *    forcing sender to re-establish connection.
   *    With new connection to receiver, right batch(ids) should be sent to receiver.  
   */
  public void doTestGatewayReConnect() throws CacheException {
    getLogWriter().info("doTestGatewayReConnect()");
    
//    final String regionName1 = this.getName() + "-1";

    // might get ServerConnectivityExceptions
    final ExpectedException expectedEx = addExpectedException(
        ServerConnectivityException.class.getName());
    // Init values at Sender.
    vm0.invoke(new CacheSerializableRunnable("Create values") {
      @Override
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(WAN_REGION_NAME);
        
        for (int i = 0; i < NUM_KEYS; i++) {
          getLogWriter().info("DEBUG: putting key-string-"+i);
          region1.put("key-string-"+i, "value-"+i, "WAN");
        }
      }
    });
    
    // Disconnect Gateway connection at Receiver. Forcing new connection from sender.
    // Gateway is connected to vm3.
    vm3.invoke(new CacheSerializableRunnable("Verify values at server2") {
      @Override
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(WAN_REGION_NAME);
        HashSet keys = new HashSet();
//        HashSet ids = new HashSet();
        
        EventListener el = (EventListener) region1.getAttributes().getCacheListener();
        
        // To disconnect in the middle of updates.
        el.waitForGatewayCallback("key-string-"+3);
        
        // Disconnect the client. From Server.
        for (Iterator iter = ClientHealthMonitor.getInstance().getClientHeartbeats().keySet().iterator();
        iter.hasNext();)
        {
          ClientHealthMonitor.getInstance().removeAllConnectionsAndUnregisterClient((ClientProxyMembershipID)iter.next());
        }
        
        // Now the  client would have established a new connection to the server.
        el.waitForGatewayCallback("key-string-"+ (NUM_KEYS - 1));
        
        Set events = el.getEvents();
        for (Iterator iter = events.iterator(); iter.hasNext();)
        {
          EntryEvent event = (EntryEvent)iter.next();
          keys.add(event.getKey());
        }
        
        if (keys.size() <=0 )
          getLogWriter().warning("Haven't received any callback events.");
        
        for (int i = 0; i < NUM_KEYS; i++) {
          assertTrue(keys.contains("key-string-"+i));
        }
       
        StatisticsType st = InternalDistributedSystem.getAnyInstance().findType("CacheServerStats");
        Statistics[] s = InternalDistributedSystem.getAnyInstance().findStatisticsByType(st);
        int NumberOfOutOfOrderBatchIds = s[0].getInt("outOfOrderGatewayBatchIds");
        getLogWriter().info("NumberOfOutOfOrderBatchIds : " + NumberOfOutOfOrderBatchIds);

        assertEquals(0, NumberOfOutOfOrderBatchIds);
      }
    });
    expectedEx.remove();
  }

  public class EventListener extends TestCacheListener implements Declarable2 {
    
    final public Set destroys = Collections.synchronizedSet(new HashSet());
    final public Set creates = Collections.synchronizedSet(new HashSet());
    final public Set invalidates = Collections.synchronizedSet(new HashSet());
    final public Set updates = Collections.synchronizedSet(new HashSet());
    final public Set events = Collections.synchronizedSet(new HashSet());
    
    @Override
    public void afterCreate2(EntryEvent event)
    {
      getLogWriter().info("DEBUG: afterCreate2 event=" + event);
      collectGatewayCallbackEvents(event);
      this.creates.add(event.getKey());
    }
    
    @Override
    public void afterDestroy2(EntryEvent event)
    {
      getLogWriter().info("DEBUG: afterDestroy2 event=" + event);
      this.destroys.add(event.getKey());
    }
    
    @Override
    public void afterInvalidate2(EntryEvent event)
    {
      getLogWriter().info("DEBUG: afterInvalidate2 event=" + event);
      this.invalidates.add(event.getKey());
    }
    
    @Override
    public void afterUpdate2(EntryEvent event)
    {
      getLogWriter().info("DEBUG: afterUpdate2 event=" + event);
      collectGatewayCallbackEvents(event);
      this.updates.add(event.getKey());
    }
    
    public void collectGatewayCallbackEvents(EntryEvent event)
    {
      synchronized(events) {
        if (((EntryEventImpl)event).getRawCallbackArgument() instanceof GatewayEventCallbackArgument)
        {
          this.events.add(event);
        }
        events.notifyAll();
      }
    }
    
    public Set getEvents()
    {
      return this.events;
    }
    
    public static final long MAX_TIME = 10000;
    
    public boolean waitForCreated(final Object key) {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return GatewayConnectionDUnitTest.EventListener.this.creates.contains(key);
        }
        public String description() {
          return "waiting for key creation: " + key;
        }
      };
      DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
      return true;
    }
    
    public boolean waitForDestroyed(final Object key) {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return GatewayConnectionDUnitTest.EventListener.this.destroys.contains(key);
        }
        public String description() {
          return "waiting for key destroy: " + key;
        }
      };
      DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
      return true;
    }
    
    public boolean waitForInvalidated(final Object key) {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return GatewayConnectionDUnitTest.EventListener.this.invalidates.contains(key);
        }
        public String description() {
          return "waiting for key invalidation: " + key;
        }
      };
      DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
      return true;
    }
    
    public boolean waitForUpdated(final Object key) {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return GatewayConnectionDUnitTest.EventListener.this.updates.contains(key);
        }
        public String description() {
          return "waiting for key update: " + key;
        }
      };
      DistributedTestCase.waitForCriterion(ev, MAX_TIME, 200, true);
      return true;
    }
    
    public void waitForGatewayCallback(Object key) {
//      final long start = System.currentTimeMillis();
      boolean foundKey = false;
      long tilt = System.currentTimeMillis() + 60 * 1000;
      
      synchronized(events) {
        while (!foundKey) {
          getLogWriter().info("DEBUG: waitForGatewayCallback events.size=" + events.size());
          for (Iterator iter = events.iterator(); iter.hasNext();)
          {
            EntryEvent event = (EntryEvent)iter.next();
            if (key.equals(event.getKey())) {
              foundKey = true;
              break;
            }
          }
          if (!foundKey) {
            try{
              events.wait(100);
            } 
            catch (InterruptedException ex){
              fail("interrupted");
            }
            if (System.currentTimeMillis() >= tilt) {
              fail("timed out");
            }
          }
        } // while
      }
    }
    
    public Properties getConfig()  {    return null;  }
    public void init(Properties props) {}  
  };
}
