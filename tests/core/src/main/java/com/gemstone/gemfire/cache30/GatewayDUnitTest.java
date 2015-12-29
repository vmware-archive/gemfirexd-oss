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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.*;

import hydra.ProcessMgr;
import dunit.*;

import java.io.*;
import java.util.*;



/**
 * Tests the WAN Gateway functionality.
 *
 * @author Kirk Lund
 * @since 4.3
 */
public class GatewayDUnitTest extends CacheTestCase {

  private static final boolean IGNORE_BUG_35596 = true;

  private static final String WAN_REGION_NAME = "GatewayDUnitTest_WAN_Root";
  private static final String DS_REGION_NAME = "GatewayDUnitTest_DS_Root";

  protected static final TxControlListener wanTxListener = new TxControlListener();
  protected static final ControlListener wanRegionListener = new ControlListener();
  protected static final ControlListener dsRegionListener = new ControlListener();

  protected static InternalLocator locator;

  private VM[] vmsDS0 = new VM[0];
  private VM[] vmsDS1 = new VM[0];

  public GatewayDUnitTest(String name) {
    super(name);
  }

  // this test has special config of distributed systems so
  // the setUp and tearDown methods need to make sure we don't
  // use the ds from previous test and that we don't leave ours around
  // for the next test to use.

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

  public void tearDown2() throws Exception {    
    
    try {
      
      //Asif: Destroy wan Queues
     destroyWanQueues();
     for (int h = 0; h < Host.getHostCount(); h++) {
       Host host = Host.getHost(h);
       for (int v = 0; v < host.getVMCount(); v++) {
         VM vm = host.getVM(v);
         vm.invoke(GatewayDUnitTest.class, "destroyWanQueues");
       }
     }
     super.tearDown2();
    }
    finally {
      disconnectAllFromDS();
      cleanupAllLocators();
    }
  }
  
  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    return factory.create();
  }

  
  public static void  destroyWanQueues() {
    try{
	    Cache cache = com.gemstone.gemfire.cache.CacheFactory.getAnyInstance();
	    for (Iterator i = cache.getGatewayHubs().iterator(); i.hasNext();) {
	      GatewayHub hub = (GatewayHub) i.next();
	      for (Iterator i1 = hub.getGateways().iterator(); i1.hasNext();) {
	        Gateway gateway = (Gateway) i1.next();
	        String rq= new StringBuffer(gateway.getGatewayHubId()).append('_').append(gateway.getId()).append("_EVENT_QUEUE").toString();
	        Region wbcl = cache.getRegion(rq);
	        if(wbcl != null) {
	          wbcl.localDestroyRegion();
	        }	        
	      }  
	    }
    } catch (CancelException cce) {
      //Ignore
    }
    
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
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    vmsDS0 = new VM[] { vm0, vm1 };
    vmsDS1 = new VM[] { vm2, vm3 };

    String hostName = getServerHostName(host);

    int[] freeUDPPorts = AvailablePortHelper.getRandomAvailableUDPPorts(2);
    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int dsPortDS0 = freeUDPPorts[0];
    int dsPortDS1 = freeUDPPorts[1];
    int hubPortDS0 = freeTCPPorts[0];
    int hubPortDS1 = freeTCPPorts[1];
    String mcastAddr0, mcastAddr1;

    if ( SocketCreator.preferIPv6Addresses() ) {
      mcastAddr0 = "FF38::1234";
      mcastAddr1 = "FF38::1235";
    } else {
      mcastAddr0 = "239.192.81.1";
      mcastAddr1 = "239.192.81.2";
    }

    setUpDS("ds0", dsPortDS0, mcastAddr0, vmsDS0, hubPortDS0,
            "ds1", hostName, hubPortDS1);
    setUpDS("ds1", dsPortDS1, mcastAddr1, vmsDS1, hubPortDS1,
            "ds0", hostName, hubPortDS0);
  }

  private void setUpDS(final String dsName,
                       final int dsPort,
                       final String mcastAddr,
                       final VM[] vms,
                       final int hubPortLocal,
                       final String dsNameRemote,
                       final String hostNameRemote,
                       final int hubPortRemote)
  throws Exception {

    // setup DS
    final Properties propsDS = new Properties();
    propsDS.setProperty(DistributionConfig.MCAST_ADDRESS_NAME, mcastAddr);
    propsDS.setProperty("mcast-port", String.valueOf(dsPort));
    propsDS.setProperty("locators", "");
    // TODO: change to co-locate locator

    // connect to DS in both vms
    for (int i = 0; i < vms.length; i++) {
      final int whichvm = i;
      final VM vm = vms[whichvm];
      vm.invoke(new CacheSerializableRunnable("Set up "+dsName) {
        public void run2() throws CacheException {
          String vmName = getSimpleName() + "_" + dsName+ "_vm" + whichvm;
          propsDS.setProperty(DistributionConfig.NAME_NAME, vmName);
          getSystem(propsDS);
          getLogWriter().info("[GatewayDUnitTest] " + vmName + " has joined " +
            dsName + " with port " + String.valueOf(dsPort));
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);

          // create WAN region
          factory.setEnableWAN(true);
          cache.getCacheTransactionManager().addListener(wanTxListener);
          factory.setCacheListener(wanRegionListener);
          cache.createRegion(WAN_REGION_NAME,
                               factory.create());

          // create DS region
          factory.setEnableWAN(false);
          factory.setCacheListener(dsRegionListener);
          cache.createRegion(DS_REGION_NAME,
                               factory.create());
          getLogWriter().info("[GatewayDUnitTest] " + vmName + " has created both regions");
        }
      });
    }

    // set up gateway in last vm
    final int whichvm = vms.length-1;
    final VM vm = vms[whichvm];

    vm.invoke(new CacheSerializableRunnable("Set up gateway in "+dsName) {
      public void run2() throws CacheException {
        String vmName = getSimpleName() + "_" + dsName+ "_vm" + whichvm;

        // create the gateway hub
        String hubName = getSimpleName() + "_"+dsName;
        String gatewayName = getSimpleName() + "_"+dsNameRemote;
        getLogWriter().info("[GatewayDUnitTest] " + vmName + " is creating " +
          hubName + " with gateway to " + gatewayName);
        Cache cache = getCache();
        GatewayHub hub = cache.setGatewayHub(hubName, hubPortLocal);
        Gateway gateway = addGateway(hub, gatewayName);

        // create the endpoint for remote DS
        getLogWriter().info("[GatewayDUnitTest] " + vmName +
          " adding endpoint [" + gatewayName + ", " + hostNameRemote + ", " +
          hubPortRemote + "] to " + gatewayName);
        gateway.addEndpoint(gatewayName, hostNameRemote, hubPortRemote);

        // create the gateway queue
        File d = new File(gatewayName + "_overflow_" + ProcessMgr.getProcessId());
        getLogWriter().info("[GatewayDUnitTest] " + vmName +
          " creating queue in " + d + " for " + gatewayName);

        GatewayQueueAttributes queueAttributes =
          new GatewayQueueAttributes(d.toString(),
            GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
            GatewayQueueAttributes.DEFAULT_BATCH_SIZE,
            GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL,
            GatewayQueueAttributes.DEFAULT_BATCH_CONFLATION,
            GatewayQueueAttributes.DEFAULT_ENABLE_PERSISTENCE,
            GatewayQueueAttributes.DEFAULT_ENABLE_ROLLING,
            GatewayQueueAttributes.DEFAULT_ALERT_THRESHOLD);
        
        // enable persistence and add disk store
        queueAttributes.setEnablePersistence(true);
        queueAttributes.setDiskStoreName(getUniqueName());
        // now create the disk store. hub.start() should succeed with ds object
        File overflowDirectory = new File("overflow_dir_"+dsName+"_vm_"+whichvm);
        overflowDirectory.mkdir();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File[] dirs1 = new File[] {overflowDirectory};
        DiskStore ds1 = dsf.setDiskDirs(dirs1).create(getUniqueName());

        gateway.setQueueAttributes(queueAttributes);
        try {
          hub.start();
        }
        catch (IOException e) {
          getLogWriter().error("Start of hub " + hubName + " threw " + e, e);
          fail("Start of hub " + hubName + " threw " + e, e);
        }
        
        getLogWriter().info("[GatewayDUnitTest] " + vmName + " has created " +
          hubName + " with gateway to " + gatewayName);
      }
    });

  }

  //////////////////////  Test Methods  //////////////////////

  /**
   * This calls many sub-tests since setUp and tearDown are so complex.
   * Gateway functionality is tested.
   */
  @SuppressWarnings("fallthrough")
  public void testGateway() throws Throwable {
    int testsCompleted = 0;
    int maxAttempts = 11;
    ExpectedException expectedEx = null;
    for (int i = 0; i < maxAttempts; i++) {
      if (i > 0) {
        // reattempt in progress due to in-use port (bug 37480 or bug 37842)
        getLogWriter().info("testGateway attempt " + i + " with " + 
                            testsCompleted + " doTests completed.");
        tearDown2();
        setUp(); // picks new unused ports for hubs
      }
      try {
        switch (testsCompleted) {
          case 0: 
            getLogWriter().info("testGateway doTestRegionContainment");
            doTestRegionContainment();
            testsCompleted++;
          // FALL THRU (to next test)   
          case 1: 
            getLogWriter().info("testGateway doTestPutThenDestroy");
            // add expected connection exception
            expectedEx = addExpectedException(ServerConnectivityException.class
                .getName());
            doTestPutThenDestroy();
            testsCompleted++;
            // fall through to next test
            
          // FALL THRU (to next test)   
          case 2:          
            getLogWriter().info("testGateway doTestDestroyFromOther");
            doTestDestroyFromOther();
            testsCompleted++;
            
          // FALL THRU (to next test)   
          case 3:
            getLogWriter().info("testGateway doTestMultipleDestroy");
            doTestMultipleDestroy();
            testsCompleted++;
            // fall through to next test

          // FALL THRU (to next test)   
          case 4:
            getLogWriter().info("testGateway doTestEarlyAckUnsupported");
            doTestEarlyAckUnsupported();
            testsCompleted++;

          // FALL THRU (to next test)   
          case 5:
            getLogWriter().info("testGateway doTestSetSocketReadTimeout");
            doTestSetSocketReadTimeout();
            testsCompleted++;
        
          // FALL THRU (to next test)   
          case 6:
            getLogWriter().info("testGateway doTestHubStopStart");
            doTestHubStopStart();
            testsCompleted++;
            
          // FALL THRU (to next test)   
          case 7:
            getLogWriter().info("testGateway doTestBatchWithException");
            doTestBatchWithException();
            testsCompleted++;
            
          // FALL THRU (to next test)   
          case 8:
            getLogWriter().info("testGateway doTestQueuePersistence");
            doTestQueuePersistence();
            testsCompleted++;
            
          // FALL THRU (to next test)   
         case 9:
            getLogWriter().info("testGateway doTestQueuePersistenceFailover");
            doTestQueuePersistenceFailover();
            testsCompleted++;

          // FALL THRU (to next test)   
          case 10:
            getLogWriter().info("testGateway doTestTxPut");
            //doTestTxPut();
            testsCompleted++;
            
          // FALL THRU (to next test)   
          case 11:
            getLogWriter().info("testGateway doRegionNotFound");
            doRegionNotFound();
            testsCompleted++;
            
          // FALL THRU (to next test/finish)   
          default:
            if (expectedEx != null) {
              expectedEx.remove();
            }
            getLogWriter().info("testGateway done");
            // done... ready to break out of for-loop
        }
        break; // completed without throwing BindException or IOException
      }
      catch (Throwable t) {
        Throwable cause = t;
        boolean handledThrowable = false;
        try {
          while (cause != null) {
            if (cause.getMessage().indexOf("BindException") > -1 ||
                cause.getMessage().indexOf("IOException") > -1) {
              // assume that IOException was caused by hub.start() due to in-use port
              handledThrowable = true;
              if (i >= maxAttempts) {
                throw new Exception("Test failed after " + i + " attempts.", t);
              }
              getLogWriter().info("testGateway about to repeat after attempt " + i + 
                  " threw IOException (see bugs 37480 and 37842).", t);
              break; // exit the while-loop since we found Bind/IOException
            }
            cause = cause.getCause();
          }
        }
        finally {
          if (!handledThrowable) {
            throw t;
          }
        }
      }
    }
  }

  private void doTestRegionContainment() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestRegionContainment");
    clearAllListeners();

    final String key = "KEY-doTestRegionContainment";
    final String val = "VAL-doTestRegionContainment";
    final String arg = "ARG-doTestRegionContainment";

    // test put
    vmPut(vmsDS0[0], DS_REGION_NAME, key, val, arg);

    waitForGatewayQueuesToEmpty();

    // make sure all other VMs in DS0 receive put
    for (int i = 0; i < vmsDS0.length; i++) {
      //vmAssertWanListener(vmsDS0[i], key, val, arg, TYPE_CREATE);
      vmsDS0[i].invoke(new CacheSerializableRunnable("Assert DS Listener") {
        public void run2() throws CacheException {
          dsRegionListener.waitWhileNoEvents(60000);
          assertTrue(!dsRegionListener.events.isEmpty());
          EventWrapper wrapper =
            (EventWrapper) dsRegionListener.events.removeFirst();
          assertNotNull(wrapper.key);
          assertNotNull(wrapper.val);
          assertNotNull(wrapper.arg);
          assertEquals(key, wrapper.key);
          assertEquals(val, wrapper.val);
          assertEquals(arg, wrapper.arg);
          assertEquals(TYPE_CREATE, wrapper.type);
        }
      });
    }

    // make sure DS1 does not receive put
    for (int i = 0; i < vmsDS1.length; i++) {
      final VM vm = vmsDS1[i];
      vm.invoke(new CacheSerializableRunnable("Verify nothing received") {
        public void run2() throws CacheException {
          assertTrue(dsRegionListener.events.isEmpty());
        }
      });
    }
  }

  private void doTestPutThenDestroy() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestPutThenDestroy");
    clearAllListeners();

    final String key = "KEY-doTestPutThenDestroy";
    final String val = "VAL-doTestPutThenDestroy";
    final String arg = "ARG-doTestPutThenDestroy";

    // test put
    vmPut(vmsDS0[0], WAN_REGION_NAME, key, val, arg);

    // make sure all other VMs in DS0 receive put
    for (int i = 0; i < vmsDS0.length; i++) {
      vmAssertWanListener(vmsDS0[i], key, val, arg, TYPE_CREATE);
    }

    // make sure all other VMs in DS1 receive put
    for (int i = 0; i < vmsDS1.length; i++) {
      vmAssertWanListener(vmsDS1[i], key, val, arg, TYPE_CREATE);
    }

    // test destroy from same vm
    vmDestroy(vmsDS0[0], WAN_REGION_NAME, key);

    // make sure all VMs in DS1 receive destroy
    for (int i = 0; i < vmsDS1.length; i++) { // was 1
      vmAssertWanListener(vmsDS1[i], key, TYPE_DESTROY);
    }

    // make sure all VMs in DS0 receive destroy
    for (int i = 0; i < vmsDS0.length; i++) {
      vmAssertWanListener(vmsDS0[i], key, TYPE_DESTROY);
    }

    waitForGatewayQueuesToEmpty();
  }

  /**
   * Make sure a tx done on an empty region gets distributed across gateways.
   * 
   * Disabled till WAN/event listeners implementation in new TX model is done.
   */
  private void DISABLED_TILL_NEW_TX_IMPL_COMPLETE_doTestTxPut() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestTxPut");
    clearAllListeners();

    final String key = "KEY-doTestTxPut";
    final String val = "VAL-doTestTxPut";
    final String arg = null;

    // @todo need ds0's region to be empty

    // test put
    vmTxPut(vmsDS0[0], WAN_REGION_NAME, key, val, arg);

    // make sure all other VMs in DS0 receive put
    // transactions on empty region will be forwarded, so
    // start from 1
    for (int i = 1; i < vmsDS0.length; i++) {
      vmAssertTxWanListener(vmsDS0[i], key, val, TYPE_CREATE);
    }

    // make sure all other VMs in DS1 receive put
    for (int i = 0; i < vmsDS1.length; i++) {
      vmAssertWanListener(vmsDS1[i], key, val, null, TYPE_CREATE);
    }

    waitForGatewayQueuesToEmpty();
  }

  private void doTestDestroyFromOther() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestDestroyFromOther");
    clearAllListeners();

    final String key = "KEY-doTestDestroyFromOther";
    final String val = "VAL-doTestDestroyFromOther";
    final String arg = "ARG-doTestDestroyFromOther";

    // test put
    vmPut(vmsDS0[0], WAN_REGION_NAME, key, val, arg);

    // make sure all other VMs in DS0 receive put
    for (int i = 0; i < vmsDS0.length; i++) {
      vmAssertWanListener(vmsDS0[i], key, val, arg, TYPE_CREATE);
    }

    // make sure all other VMs in DS1 receive put
    for (int i = 0; i < vmsDS1.length; i++) {
      vmAssertWanListener(vmsDS1[i], key, val, arg, TYPE_CREATE);
    }

    // test destroy from other side
    vmDestroy(vmsDS1[0], WAN_REGION_NAME, key);

    // make sure all VMs in DS1 receive destroy
    for (int i = 0; i < vmsDS1.length; i++) { // was 1
      vmAssertWanListener(vmsDS1[i], key, TYPE_DESTROY);
    }

    // make sure all VMs in DS0 receive destroy
    for (int i = 0; i < vmsDS0.length; i++) {
      vmAssertWanListener(vmsDS0[i], key, TYPE_DESTROY);
    }

    waitForGatewayQueuesToEmpty();
  }

  private void doTestMultipleDestroy() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestMultipleDestroy");
    clearAllListeners();

    final String key = "KEY-doTestMultipleDestroy";
    final String val = "VAL-doTestMultipleDestroy";
    final String arg = "ARG-doTestMultipleDestroy";
    final int entryCount = 100;

    // test many puts
    VM vmPut = vmsDS0[0];
    vmPut.invoke(new CacheSerializableRunnable("Perform puts") {
      public void run2() throws CacheException {
        Region dsRegion = getCache().getRegion(WAN_REGION_NAME);
        for (int i = 0; i < entryCount; i++) {
          dsRegion.put(key+i, val+i, arg+i);
        }
      }
    });

    final SerializableRunnable assertPuts =
      new CacheSerializableRunnable("Verify receipt of puts") {
        public void run2() throws CacheException {
          for (int i = 0; i < entryCount; i++) {
            assertWanListener(key+i, val+i, arg+i, TYPE_CREATE);
          }
        }
      };

    // make sure all other VMs in DS0 receive puts
    for (int i = 0; i < vmsDS0.length; i++) { // was 1
      vmsDS0[i].invoke(assertPuts);
    }

    // make sure all other VMs in DS1 receive put
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(assertPuts);
    }

    clearAllListeners();

    // test destroy from both sides
    VM vmDestroy0 = vmsDS0[0];
    vmDestroy0.invokeAsync(new CacheSerializableRunnable("Perform destroys forewards") {
      public void run2() throws CacheException {
        Region dsRegion = getCache().getRegion(WAN_REGION_NAME);
        try {
          for (int i = 0; i < entryCount; i++) {
            dsRegion.destroy(key+i);
          }
        }
        catch (EntryNotFoundException ex) {
          // It is possible to get this exception if the destroy backwards
          // threads' destroys reach us before ours are performed.
        }
      }
    });

    VM vmDestroy1 = vmsDS1[0];
    vmDestroy1.invokeAsync(new CacheSerializableRunnable("Perform destroys backwards") {
      public void run2() throws CacheException {
        Region dsRegion = getCache().getRegion(WAN_REGION_NAME);
        try {
          for (int i = entryCount-1; i > -1; i--) {
            dsRegion.destroy(key+i);
          }
        }
        catch (EntryNotFoundException ex) {
          // It is possible to get this exception if the destroy forwards
          // threads' destroys reach us before ours are performed.
        }
      }
    });

    waitForGatewayQueuesToEmpty();

    final SerializableRunnable assertDestroys =
      new CacheSerializableRunnable("Verify receipt of destroys") {
        public void run2() throws CacheException {
          Set allKeys = new HashSet();
          for (int i = 0; i < entryCount; i++) {
            allKeys.add(key+i);
          }
          for (int i = 0; i < entryCount; i++) {
            wanRegionListener.waitWhileNoEvents(60000);
            assertTrue(!wanRegionListener.events.isEmpty());
            EventWrapper wrapper =
              (EventWrapper) wanRegionListener.events.removeFirst();
            assertNotNull(wrapper.key);
            assertEquals(TYPE_DESTROY, wrapper.type);
            allKeys.remove(wrapper.key);
          }
          assertTrue(allKeys.isEmpty());
        }
      };

    // make sure all VMs in DS1 receive destroy
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(assertDestroys);
    }

    // make sure all VMs in DS0 receive destroy
    for (int i = 0; i < vmsDS0.length; i++) {
      vmsDS0[i].invoke(assertDestroys);
    }
    waitForGatewayQueuesToEmpty();
  }

  private void doTestBatchWithException() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestBatchWithException");
    clearAllListeners();

    final String key = "KEY-doTestBatchWithException";
    final String val = "VAL-doTestBatchWithException";
    final String arg = "ARG-doTestBatchWithException";
    final int entryCount = 20;

    VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    VM hubVMDS1 = vmsDS1[vmsDS1.length-1];

    // reconfigure hubs for batch size 20 and high time interval
    SerializableRunnable configureGatewayBatching =
      new CacheSerializableRunnable("configureGatewayBatching") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway batch size and interval
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(entryCount);
          queueAttributes.setBatchTimeInterval(Integer.MAX_VALUE);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e, e);
          }
        }
      };

    SerializableRunnable restoreGatewayDefaults =
      new CacheSerializableRunnable("restoreGatewayDefaults") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway with defaults
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(GatewayQueueAttributes.DEFAULT_BATCH_SIZE);
          queueAttributes.setBatchTimeInterval(GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e, e);
          }
        }
      };

    try {
      hubVMDS0.invoke(configureGatewayBatching);
      hubVMDS1.invoke(configureGatewayBatching);

      // perform puts
      vmsDS0[0].invoke(new CacheSerializableRunnable("Perform puts") {
        public void run2() throws CacheException {
          Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
          for (int i = 0; i < entryCount-1; i++) {
            wanRegion.put(key+i, val+i, arg+i);
          }
        }
      });

      // verify that batch has NOT gone thru
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(new CacheSerializableRunnable("Verify no events yet") {
          public void run2() throws CacheException {
            wanRegionListener.waitWhileNoEvents(
                GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
            assertTrue(wanRegionListener.events.isEmpty());
          }
        });
      }

      vmsDS0[0].invoke(new CacheSerializableRunnable("Perform last put") {
        public void run2() throws CacheException {
          Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
          int i = entryCount-1;
          wanRegion.put(key+i, val+i, arg+i);
        }
      });

      final SerializableRunnable assertPuts =
        new CacheSerializableRunnable("Verify receipt of puts") {
          public void run2() throws CacheException {
            for (int i = 0; i < entryCount; i++) {
              assertWanListener(key+i, val+i, arg+i, TYPE_CREATE);
            }
          }
        };

      // make sure all other VMs in DS0 receive puts
      for (int i = 0; i < vmsDS0.length; i++) {
        vmsDS0[i].invoke(assertPuts);
      }

      // make sure all other VMs in DS1 receive put
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(assertPuts);
      }

      clearAllListeners();

      // destroy and recreate WAN region in DS1
      final SerializableRunnable destroyWanRegion =
        new CacheSerializableRunnable("Destroy WAN Region") {
          public void run2() throws CacheException {
          // destroy WAN region
          Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
          wanRegion.localDestroyRegion();
          clearListeners();
        }
      };

      final SerializableRunnable recreateWanRegion =
        new CacheSerializableRunnable("Recreate WAN Region") {
          public void run2() throws CacheException {
          // recreate WAN region
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setEnableWAN(true);
          getCache().getCacheTransactionManager().addListener(wanTxListener);
          factory.setCacheListener(wanRegionListener);
          getCache().createRegion(
            WAN_REGION_NAME, factory.create());
          clearListeners();
        }
      };

      // stop gateway hub
      hubVMDS1.invoke(new CacheSerializableRunnable("Stop GatewayHub") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();
        }
      });

      // recreate regions to empty them out
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(destroyWanRegion);
      }
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(recreateWanRegion);
      }

      // assert that WAN region is empty in ds1 -- why isn't the region empty?!
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(new CacheSerializableRunnable("Assert WAN region empty") {
          public void run2() throws CacheException {
            Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
            assertEquals("WAN region size should be zero",
                         0, wanRegion.keys().size());
          }
        });
      }

      // restart gateway hub
      hubVMDS1.invoke(new CacheSerializableRunnable("Restart GatewayHub") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e, e);
          }
        }
      });

      // build up batch with destroys and puts
      vmsDS0[0].invoke(new CacheSerializableRunnable("Perform puts") {
        public void run2() throws CacheException {
          Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
          // 1st op in batch is a destroy
          wanRegion.destroy(key+0);
          // ops 2-20 are new puts with larger integers
          for (int i = 1; i < entryCount; i++) {
            int j = i*100;
            wanRegion.put(key+j, val+j, arg+j);
          }
        }
      });

      waitForGatewayQueuesToEmpty();

      // assert all non-destroy events are processed by DS1
      final SerializableRunnable assertNewPuts =
        new CacheSerializableRunnable("Verify receipt of newer puts") {
          public void run2() throws CacheException {
            for (int i = 0; i < entryCount; i++) {
              int j = i*100;
              assertWanListener(key+j, val+j, arg+j, i==0? TYPE_DESTROY : TYPE_CREATE);
            }
          }
        };

      // make sure all other VMs in DS1 receive new puts
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(assertNewPuts);
      }

    }
    finally {
      hubVMDS0.invoke(restoreGatewayDefaults);
      hubVMDS1.invoke(restoreGatewayDefaults);
    }
  }

  /**
   * Test basic queue persistence of gateway batch. 
   */
  private void doTestQueuePersistence() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestQueuePersistence");
    clearAllListeners();

    final String key = "KEY-doTestQueuePersistence";
    final String val = "VAL-doTestQueuePersistence";
    final String arg = "ARG-doTestQueuePersistence";
    final int entryCount = 20;

    VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    VM hubVMDS1 = vmsDS1[vmsDS1.length-1];

    // reconfigure hubs for batch size 20 and high time interval
    SerializableRunnable configureGatewayBatching =
      new CacheSerializableRunnable("configureGatewayBatching") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway batch size and interval
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(entryCount);
          queueAttributes.setBatchTimeInterval(Integer.MAX_VALUE);
          queueAttributes.setEnablePersistence(true);
          gateway.setQueueAttributes(queueAttributes);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e, e);
          }
        }
      };

    SerializableRunnable restoreGatewayDefaults =
      new CacheSerializableRunnable("restoreGatewayDefaults") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway with defaults
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(GatewayQueueAttributes.DEFAULT_BATCH_SIZE);
          queueAttributes.setBatchTimeInterval(GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
          queueAttributes.setEnablePersistence(false);
          gateway.setQueueAttributes(queueAttributes);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e);
          }
        }
      };

    try {
      // reconfigure hubs for batching
      hubVMDS0.invoke(configureGatewayBatching);
      hubVMDS1.invoke(configureGatewayBatching);
      
      // stop DS1 hub
      vmStopHub(hubVMDS1);

      // perform puts
      vmsDS0[0].invoke(new CacheSerializableRunnable("Perform puts") {
        public void run2() throws CacheException {
          Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
          for (int i = 0; i < entryCount; i++) {
            wanRegion.put(key+i, val+i, arg+i);
          }
        }
      });

      // verify that batch has NOT gone thru
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(new CacheSerializableRunnable("Verify no events yet") {
          public void run2() throws CacheException {
            wanRegionListener.waitWhileNoEvents(
                GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
            assertTrue(wanRegionListener.events.isEmpty());
          }
        });
      }
      
      final SerializableRunnable assertPuts =
        new CacheSerializableRunnable("Verify receipt of puts") {
          public void run2() throws CacheException {
            for (int i = 0; i < entryCount; i++) {
              assertWanListener(key+i, val+i, arg+i, TYPE_CREATE);
            }
          }
        };

      // make sure all other VMs in DS0 receive puts
      for (int i = 0; i < vmsDS0.length; i++) {
        vmsDS0[i].invoke(assertPuts);
      }

      // stop and restart DS0 hub to test loading of persisted queue
      vmStopHub(hubVMDS0);
      vmStartHub(hubVMDS0);

      // restart DS1 hub to make sure queued batch gets delivered
      vmStartHub(hubVMDS1);

      waitForGatewayQueuesToEmpty();

      // make sure all other VMs in DS1 receive put
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(assertPuts);
      }

    }
    finally {
      hubVMDS0.invoke(restoreGatewayDefaults);
      hubVMDS1.invoke(restoreGatewayDefaults);
    }
  }

  /**
   * Test failover of queue persistence to secondary gateway. 
   */
  private void doTestQueuePersistenceFailover() throws Exception {
    /* these are expected...
    dunit-tests-1005-094309/bgexec603_28278.log: [severe 2007/10/05 09:44:48.523 PDT <Gateway Event Processor from GatewayDUnitTest_ds1 to GatewayDUnitTest_ds0-0x6e76> nid=0xcf75aba0] Uncaught exception in thread <Gateway Event Processor from GatewayDUnitTest_ds1 to GatewayDUnitTest_ds0>
    com.gemstone.gemfire.cache.RegionDestroyedException: com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue$SingleReadWriteMetaRegion[path='/GatewayDUnitTest_ds1_GatewayDUnitTest_ds0_EVENT_QUEUE';scope=DISTRIBUTED_ACK';dataPolicy=REPLICATE; gatewayEnabled=false]
          at com.gemstone.gemfire.internal.cache.LocalRegion.checkRegionDestroyed(LocalRegion.java:4547)
          at com.gemstone.gemfire.internal.cache.LocalRegion.checkReadiness(LocalRegion.java:1729)
          at com.gemstone.gemfire.internal.cache.LocalRegion.values(LocalRegion.java:1196)
          at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.handleFailover(GatewayImpl.java:1832)
          at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.run(GatewayImpl.java:1270)
  
    gemfire2_28278/system.log: [severe 2007/10/05 09:44:48.523 PDT GatewayDUnitTest_ds1_vm1 <Gateway Event Processor from GatewayDUnitTest_ds1 to GatewayDUnitTest_ds0-0x6e76> nid=0xcf75aba0] Uncaught exception in thread <Gateway Event Processor from GatewayDUnitTest_ds1 to GatewayDUnitTest_ds0>
    com.gemstone.gemfire.cache.RegionDestroyedException: com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue$SingleReadWriteMetaRegion[path='/GatewayDUnitTest_ds1_GatewayDUnitTest_ds0_EVENT_QUEUE';scope=DISTRIBUTED_ACK';dataPolicy=REPLICATE; gatewayEnabled=false]
          at com.gemstone.gemfire.internal.cache.LocalRegion.checkRegionDestroyed(LocalRegion.java:4547)
          at com.gemstone.gemfire.internal.cache.LocalRegion.checkReadiness(LocalRegion.java:1729)
          at com.gemstone.gemfire.internal.cache.LocalRegion.values(LocalRegion.java:1196)
          at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.handleFailover(GatewayImpl.java:1832)
          at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.run(GatewayImpl.java:1270)
    */
    final String expected = 
      "com.gemstone.gemfire.cache.RegionDestroyedException";
    final String addExpected = 
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected = 
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";
    
    getLogWriter().info("[GatewayDUnitTest] doTestQueuePersistenceFailover");
    clearAllListeners();
    getLogWriter().info("[GatewayDUnitTest] after clear all listeners");

    final String key = "KEY-doTestQueuePersistenceFailover";
    final String val = "VAL-doTestQueuePersistenceFailover";
    final String arg = "ARG-doTestQueuePersistenceFailover";
    final int entryCount = 20;

    final int hubVMDS0_Secondary_vmNumber = 0;
    final int hubVMDS1_Secondary_vmNumber = 0;
      
    final VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    final VM hubVMDS0_Secondary = vmsDS0[hubVMDS0_Secondary_vmNumber];
    
    final VM hubVMDS1 = vmsDS1[vmsDS1.length-1];
    final VM hubVMDS1_Secondary = vmsDS1[hubVMDS1_Secondary_vmNumber];

    int[] freePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int hubVMDS0_Secondary_port = freePorts[0];
    final int hubVMDS1_Secondary_port = freePorts[1];

    // reconfigure hubs for batch size 20 and high time interval
    final SerializableRunnable configureGatewayBatching =
      new CacheSerializableRunnable("configureGatewayBatching") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway batch size and interval
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(entryCount);
          queueAttributes.setBatchTimeInterval(Integer.MAX_VALUE);
          queueAttributes.setEnablePersistence(true);
          gateway.setQueueAttributes(queueAttributes);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e);
          }
        }
      };

    final SerializableRunnable restoreGatewayDefaults =
      new CacheSerializableRunnable("restoreGatewayDefaults") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway with defaults
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(GatewayQueueAttributes.DEFAULT_BATCH_SIZE);
          queueAttributes.setBatchTimeInterval(GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
          queueAttributes.setEnablePersistence(false);
          gateway.setQueueAttributes(queueAttributes);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e);
          }
        }
      };

    final SerializableRunnable createSecondaryGateway = // TODO bug36296
      new CacheSerializableRunnable("createSecondaryGateway") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();

          // reconfigure gateway with defaults
          Gateway gateway = hub.getGateways().iterator().next();
          GatewayQueueAttributes queueAttributes = gateway.getQueueAttributes();
          queueAttributes.setBatchSize(GatewayQueueAttributes.DEFAULT_BATCH_SIZE);
          queueAttributes.setBatchTimeInterval(GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
          queueAttributes.setEnablePersistence(true);
          gateway.setQueueAttributes(queueAttributes);

          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e);
          }
        }
      };
      
    //final VM gatewayDUnitTest_ds0_vm0 = Host.getHost(0).getVM(0);
    //final VM gatewayDUnitTest_ds0_vm1 = Host.getHost(0).getVM(1);
    //final VM gatewayDUnitTest_ds1_vm0 = Host.getHost(0).getVM(2);
    final VM gatewayDUnitTest_ds1_vm1 = Host.getHost(0).getVM(3);
    boolean done = false;
    try {
      // reconfigure hubs for batching
      hubVMDS0.invoke(configureGatewayBatching);
    getLogWriter().info("[GatewayDUnitTest] after hubVMDS0.invoke(configureGatewayBatching)");
      hubVMDS1.invoke(configureGatewayBatching);
    getLogWriter().info("[GatewayDUnitTest] after hubVMDS1.invoke(configureGatewayBatching)");
      
      // configure secondary gateway in DS0
      final String nameDS1 = "ds1";
        //hubVMDS1.invoke(GatewayDUnitTest.class, "getSystemName");
      final String hostDS1 = (String)
        hubVMDS1.invoke(GatewayDUnitTest.class, "getHostName");
    getLogWriter().info("[GatewayDUnitTest] after hubVMDS1 getHostName");
      final int portDS1 = 
        hubVMDS1.invokeInt(GatewayDUnitTest.class, "getHubPort");
    getLogWriter().info("[GatewayDUnitTest] after hubVMDS1 getHubPort");
        
      vmCreateGateway(
        hubVMDS0_Secondary, "ds0", 
        hubVMDS0_Secondary_vmNumber, hubVMDS0_Secondary_port,
        nameDS1, hostDS1, new int[] { portDS1, hubVMDS1_Secondary_port }, true);
    getLogWriter().info("[GatewayDUnitTest] after vmCreateGateway ds0");
      hubVMDS0_Secondary.invoke(configureGatewayBatching);
    getLogWriter().info("[GatewayDUnitTest] after hubVMDS0_Secondary.invoke(configureGatewayBatching)");

      // configure secondary gateway in DS1
      final String nameDS0 = "ds0";
        //hubVMDS0.invoke(GatewayDUnitTest.class, "getSystemName");
      final String hostDS0 = (String)
        hubVMDS0.invoke(GatewayDUnitTest.class, "getHostName");
      final int portDS0 = 
        hubVMDS0.invokeInt(GatewayDUnitTest.class, "getHubPort");
        
      vmCreateGateway( 
        hubVMDS1_Secondary, "ds1",
        hubVMDS1_Secondary_vmNumber, hubVMDS1_Secondary_port,
        nameDS0, hostDS0, new int[] { portDS0, hubVMDS0_Secondary_port }, true);
    getLogWriter().info("[GatewayDUnitTest] after vmCreateGateway ds1");
      hubVMDS1_Secondary.invoke(configureGatewayBatching);
    getLogWriter().info("[GatewayDUnitTest] after hubVMDS1_Secondary.invoke(configureGatewayBatching)");
      
      // stop DS1 hubs
      vmStopHub(hubVMDS1);
    getLogWriter().info("[GatewayDUnitTest] after vmStopHub(hubVMDS1)");
      vmStopHub(hubVMDS1_Secondary);
    getLogWriter().info("[GatewayDUnitTest] after vmStopHub(hubVMDS1_Secondary)");

      // perform puts
      vmsDS0[0].invoke(new CacheSerializableRunnable("Perform puts") {
        public void run2() throws CacheException {
          Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
          for (int i = 0; i < entryCount; i++) {
            wanRegion.put(key+i, val+i, arg+i);
          }
        }
      });
    getLogWriter().info("[GatewayDUnitTest] after perform puts");

      // verify that batch has NOT gone thru
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(new CacheSerializableRunnable("Verify no events yet") {
          public void run2() throws CacheException {
            wanRegionListener.waitWhileNoEvents(
                GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
            assertTrue(wanRegionListener.events.isEmpty());
          }
        });
      }
    getLogWriter().info("[GatewayDUnitTest] after verify");
      
      final SerializableRunnable assertPuts =
        new CacheSerializableRunnable("Verify receipt of puts") {
          public void run2() throws CacheException {
            wanRegionListener.waitWhileNotEnoughEvents(60000, entryCount);
            assertEquals(entryCount, wanRegionListener.events.size());
            for (int i = 0; i < entryCount; i++) {
              assertWanListenerNow(key+i, val+i, arg+i, TYPE_CREATE);
            }
          }
        };

      // make sure all VMs in DS0 receive puts
      for (int i = 0; i < vmsDS0.length; i++) {
        vmsDS0[i].invoke(assertPuts);
    getLogWriter().info("[GatewayDUnitTest] after assertPuts#" + i);
      }

      // stop and restart DS0 hubs to test loading of persisted queue
      vmStopHub(hubVMDS0);
    getLogWriter().info("[GatewayDUnitTest] after vmStopHub(hubVMDS0)");
      vmStopHub(hubVMDS0_Secondary);
    getLogWriter().info("[GatewayDUnitTest] after vmStopHub(hubVMDS0_Secondary)");
      vmStartHub(hubVMDS0_Secondary);
    getLogWriter().info("[GatewayDUnitTest] after vmStartHub(hubVMDS0_Secondary)");

      // restart DS1 hub to make sure queued batch gets delivered
      addExpected(gatewayDUnitTest_ds1_vm1, addExpected);
    
      vmStartHub(hubVMDS1_Secondary);
    getLogWriter().info("[GatewayDUnitTest] after vmStartHub(hubVMDS1_Secondary)");
      
      // give a little time for events to be delivered
      waitForGatewayQueuesToEmpty();
      //pause(5000);

      // make sure all other VMs in DS1 receive put
      for (int i = 0; i < vmsDS1.length; i++) {
        vmsDS1[i].invoke(assertPuts);
        getLogWriter().info("[GatewayDUnitTest] after vmsDS1[" + i + "] assertPuts");
      }

      done = true;
    }
    finally {
      removeExpected(gatewayDUnitTest_ds1_vm1, removeExpected);
      try {
      vmStopHub(hubVMDS0_Secondary);
      getLogWriter().info("[GatewayDUnitTest] after vmStopHub(hubVMDS0_Secondary)");
      vmStopHub(hubVMDS1_Secondary);
      getLogWriter().info("[GatewayDUnitTest] after vmStopHub(hubVMDS1_Secondary)");
      hubVMDS0.invoke(restoreGatewayDefaults);
        getLogWriter().info("[GatewayDUnitTest] after hubVMDS0.invoke(restoreGatewayDefaults)");
      hubVMDS1.invoke(restoreGatewayDefaults);
        getLogWriter().info("[GatewayDUnitTest] after hubVMDS1.invoke(restoreGatewayDefaults)");
      } catch(Exception e) {
        //only throw an exception if there was not an earlier test exception.
        if(done) {
          throw e;
        }
      }
    }
  }

  private void doRegionNotFound() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doRegionNotFound");
    clearAllListeners();

    final String key = "KEY-doRegionNotFound";
    final String val = "VAL-doRegionNotFound";
    final String arg = "ARG-doRegionNotFound";
    final int entryCount = 10;

    // recreate regions to clean them out (the tx test converts the region to DataPolicy.EMPTY)
    // destroy and recreate WAN region
    final SerializableRunnable destroyWanRegion =
      new CacheSerializableRunnable("Destroy WAN Region") {
        public void run2() throws CacheException {
        // destroy WAN region
        Region wanRegion = getCache().getRegion(WAN_REGION_NAME);
        wanRegion.localDestroyRegion();
        clearListeners();
      }
    };

    final SerializableRunnable recreateWanRegion =
      new CacheSerializableRunnable("Recreate WAN Region") {
        public void run2() throws CacheException {
        // recreate WAN region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        factory.setEnableWAN(true);
        getCache().getCacheTransactionManager().addListener(wanTxListener);
        factory.setCacheListener(wanRegionListener);
        getCache().createRegion(
          WAN_REGION_NAME, factory.create());
        clearListeners();
      }
    };

    for (int i = 0; i < vmsDS0.length; i++) {
      vmsDS0[i].invoke(destroyWanRegion);
    }
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(destroyWanRegion);
    }
    
    for (int i = 0; i < vmsDS0.length; i++) {
      vmsDS0[i].invoke(recreateWanRegion);
    }
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(recreateWanRegion);
    }
    
    final SerializableRunnable doPuts = 
      new CacheSerializableRunnable("Perform puts") {
        public void run2() throws CacheException {
          Region dsRegion = getCache().getRegion(WAN_REGION_NAME);
          for (int i = 0; i < entryCount; i++) {
            dsRegion.put(key+i, val+i, arg+i);
          }
        }
      };
    vmsDS0[0].invoke(doPuts);

    final SerializableRunnable assertCreates =
      new CacheSerializableRunnable("Verify receipt of creates") {
        public void run2() throws CacheException {
          for (int i = 0; i < entryCount; i++) {
            assertWanListener(key+i, val+i, arg+i, TYPE_CREATE);
          }
        }
      };

    // make sure all VMs in DS0 receive puts
    for (int i = 0; i < vmsDS0.length; i++) {
      vmsDS0[i].invoke(assertCreates);
    }

    // make sure all VMs in DS1 receive put
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(assertCreates);
    }

    // Destroy the region in the DS1 VMs
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(destroyWanRegion);
    }

    // add expected exceptions
    final String[] expectedExceptions = new String[] {
      "com.gemstone.gemfire.cache.RegionDestroyedException",
      "com.gemstone.gemfire.internal.cache.tier.BatchException",
      "com.gemstone.gemfire.cache.client.ServerOperationException",
    };
    for (int i = 0; i < vmsDS0.length; i++) {
      for (int j = 0; j < expectedExceptions.length; j++) {
        addExpected(vmsDS0[i], "<ExpectedException action=add>" + expectedExceptions[j] + "</ExpectedException>");
      }
    }
    for (int i = 0; i < vmsDS1.length; i++) {
      for (int j = 0; j < expectedExceptions.length; j++) {
        addExpected(vmsDS1[i], "<ExpectedException action=add>" + expectedExceptions[j] + "</ExpectedException>");
      }
    }

    try {
      // test put again
      vmsDS0[0].invoke(doPuts);
  
      final SerializableRunnable assertUpdates =
        new CacheSerializableRunnable("Verify receipt of updates") {
          public void run2() throws CacheException {
            for (int i = 0; i < entryCount; i++) {
              assertWanListener(key+i, val+i, arg+i, TYPE_UPDATE);
            }
          }
        };
  
      // make sure all VMs in DS0 receive put
      for (int i = 0; i < vmsDS0.length; i++) {
        vmsDS0[i].invoke(assertUpdates);
      }
  
      // verify the event was removed from all the queues
      // this means that the RegionNotFoundException was handled properly
      waitForGatewayQueuesToEmpty();
    } finally {
      // remove expected exceptions
      for (int i = 0; i < vmsDS0.length; i++) {
        for (int j = 0; j < expectedExceptions.length; j++) {
          addExpected(vmsDS0[i], "<ExpectedException action=remove>" + expectedExceptions[j] + "</ExpectedException>");
        }
      }
      for (int i = 0; i < vmsDS1.length; i++) {
        for (int j = 0; j < expectedExceptions.length; j++) {
          addExpected(vmsDS1[i], "<ExpectedException action=remove>" + expectedExceptions[j] + "</ExpectedException>");
        }
      }
    }
  }
  
  private void addExpected(VM vm, final String addExpected) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        getCache().getLogger().info(addExpected);
      }
    });
  }
  private void removeExpected(VM vm, final String removeExpected) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        getCache().getLogger().info(removeExpected);
      }
    });
  }

  /**
   * blocks until all outgoing queued batches have been sent successfully.
   */
  private void waitForGatewayQueuesToEmpty() {
    SerializableRunnable waitForQueueToDrain =
      new CacheSerializableRunnable("waitForQueueToDrain") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          Iterator it = hub.getGateways().iterator();
          while (it.hasNext()) {
            final Gateway gateway = (Gateway)it.next();
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                return !gateway.isRunning() || gateway.getQueueSize() == 0;
              }
              public String description() {
                return "Waiting on gateway " + gateway;
              }
            };
            DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
          }
        }
      };

    VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    VM hubVMDS1 = vmsDS1[vmsDS1.length-1];
    hubVMDS0.invoke(waitForQueueToDrain);
    hubVMDS1.invoke(waitForQueueToDrain);
  }
  
  /**
   * Verifies that Gateway setEarlyAck(true) now throws
   * UnsupportedOperationException.
   */
  private void doTestEarlyAckUnsupported() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestEarlyAckNotAllowed");

    VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    VM hubVMDS1 = vmsDS1[vmsDS1.length-1];

    SerializableRunnable makeEarlyAck =
      new CacheSerializableRunnable("makeEarlyAck") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();
          Gateway gateway = hub.getGateways().iterator().next();
          try {
            gateway.setEarlyAck(true);
            fail("Gateway setEarlyAck(true) should throw UnsupportedOperationException");
          }
          catch (UnsupportedOperationException e) {
            // pass... setEarlyAck is no longer supported
          }
          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e);
          }
        }
      };

    hubVMDS0.invoke(makeEarlyAck);
    hubVMDS1.invoke(makeEarlyAck);
  }

  /**
   * Verifies that Gateway setSocketReadTimeout does nothing.
   */
  private void doTestSetSocketReadTimeout() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestSetSocketReadTimeout");

    VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    VM hubVMDS1 = vmsDS1[vmsDS1.length-1];

    SerializableRunnable setSocketReadTimeout =
      new CacheSerializableRunnable("setSocketReadTimeout") {
        public void run2() throws CacheException {
          List hubs = getCache().getGatewayHubs();
          GatewayHub hub = (GatewayHub)hubs.get(0);
          hub.stop();
          Gateway gateway = hub.getGateways().iterator().next();
          gateway.setSocketReadTimeout(10000);
          assertEquals("Gateway setSocketReadTimeout should do nothing",
                       0, gateway.getSocketReadTimeout());
          try {
            hub.start();
          }
          catch (IOException e) {
            getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
            fail("Start of hub " + hub.getId() + " threw " + e, e);
          }
        }
      };

    hubVMDS0.invoke(setSocketReadTimeout);
    hubVMDS1.invoke(setSocketReadTimeout);
  }

  /**
   * Verifies that GatewayHub can be stopped and restarted.
   */
  private void doTestHubStopStart() throws Exception {
    getLogWriter().info("[GatewayDUnitTest] doTestHubStopStart");
    clearAllListeners();

    final String key = "KEY-doTestHubStopStart";
    final String val = "VAL-doTestHubStopStart";
    final String arg = "ARG-doTestHubStopStart";

    VM hubVMDS0 = vmsDS0[vmsDS0.length-1];
    VM hubVMDS1 = vmsDS1[vmsDS1.length-1];

    // basic stop and then start
    vmStopHub(hubVMDS0); //.invoke(hubStop);
    vmStartHub(hubVMDS0); //.invoke(hubStart);

    // make sure no events come thru while stopped
    vmStopHub(hubVMDS1); //.invoke(hubStop);

    // test put
    vmPut(vmsDS0[0], WAN_REGION_NAME, key, val, arg);

    // make sure all VMs in DS0 receive put
    for (int i = 0; i < vmsDS0.length; i++) {
      vmAssertWanListener(vmsDS0[i], key, val, arg, TYPE_CREATE);
    }

    // make sure all VMs in DS1 do NOT receive put (hub is stopped
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(new CacheSerializableRunnable("Verify no events yet") {
        public void run2() throws CacheException {
          wanRegionListener.waitWhileNoEvents(
              GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL);
          assertTrue(wanRegionListener.events.isEmpty());
        }
      });
    }

    // restart the hub
    vmStartHub(hubVMDS1); //.invoke(hubStart);

    waitForGatewayQueuesToEmpty();

    // need to empty out listeners
    for (int i = 0; i < vmsDS1.length; i++) {
      vmsDS1[i].invoke(new CacheSerializableRunnable("Clear out events") {
        public void run2() throws CacheException {
          wanRegionListener.waitWhileNoEvents(60000);
          clearListeners();
        }
      });
    }
  }

  private void vmPut(VM vm, final String regionName, final Object key, final Object val, final Object arg)
  throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmPut") {
      public void run2() throws CacheException {
        Region dsRegion = getCache().getRegion(regionName);
        dsRegion.put(key, val, arg);
      }
    });
  }
  private void vmTxPut(VM vm, final String regionName, final Object key, final Object val, final Object arg)
  throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmTxPut") {
      public void run2() throws CacheException {
        Region dsRegion = getCache().getRegion(regionName);
        // need to destroy this region and recreate with empty DataPolicy
        String dsName = dsRegion.getName();
        RegionAttributes ra = dsRegion.getAttributes();
        AttributesFactory af = new AttributesFactory(ra);
        af.setDataPolicy(DataPolicy.EMPTY);
        ra = af.create();
        Region parent = dsRegion.getParentRegion();
        dsRegion.localDestroyRegion();
        if (parent == null) {
          dsRegion = getCache().createRegion(dsName, ra);
        } else {
          dsRegion = parent.createSubregion(dsName, ra);
        }
        getCache().getCacheTransactionManager().begin();
        try {
          dsRegion.put(key, val, arg);
        } finally {
          getCache().getCacheTransactionManager().commit();
        }
      }
    });
  }

  private void vmDestroy(VM vm, final String regionName, final Object key)
  throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmDestroy") {
      public void run2() throws CacheException {
        Region dsRegion = getCache().getRegion(regionName);
        dsRegion.destroy(key);
      }
    });
  }

  private void vmAssertWanListener(VM vm, final Object key, final Object val, final Object arg, final int type)
  throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmAssertWanListener") {
      public void run2() throws CacheException {
        assertWanListener(key, val, arg, type);
      }
    });
  }

  private void vmAssertWanListener(VM vm, final Object key, final int type)
  throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmAssertWanListener") {
      public void run2() throws CacheException {
        assertWanListener(key, type);
      }
    });
  }

  private void vmAssertTxWanListener(VM vm, final Object key, final Object val, final int type)
  throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmAssertTxWanListener") {
      public void run2() throws CacheException {
        assertTxWanListener(key, val, type);
      }
    });
  }


  private static String getSystemName() throws Exception {
    return InternalDistributedSystem.getAnyInstance().getName();
  }
  private static String getHostName() throws Exception {
    String hostName = System.getProperty("gemfire.bind-address");
    if (hostName == null) {
      try {
        hostName = DistributedTestCase.getIPLiteral();
      }
      catch (Exception e) {
        getLogWriter().error("InetAddress threw Exception", e);
        fail("InetAddress threw Exception" + e.getMessage());
      }
    }
    return hostName;
  }
  private static int getHubPort() throws Exception {
    Cache c = CacheFactory.getAnyInstance();
    List hubs = c.getGatewayHubs();
    GatewayHub hub = (GatewayHub)hubs.get(0);
    return hub.getPort();
  }
  
  private void vmStopHub(VM vm) throws Exception {
    assertNotNull("vmStopHub called with null vm", vm);
    vm.invoke(new CacheSerializableRunnable("vmStopHub") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        assertNotNull("getCache returned null for vmStopHub", cache);
        List hubs = getCache().getGatewayHubs();
        assertTrue( "no hubs for vmStopHub", hubs.size() > 0);
        GatewayHub hub = (GatewayHub)hubs.get(0);
        hub.stop();
      }
    });
  }

  private void vmStartHub(VM vm) throws Exception {
    vm.invoke(new CacheSerializableRunnable("vmStartHub") {
      public void run2() throws CacheException {
        List hubs = getCache().getGatewayHubs();
        GatewayHub hub = (GatewayHub)hubs.get(0);
        try {
          hub.start();
        }
        catch (IOException e) {
          getLogWriter().error("Start of hub " + hub.getId() + " threw " + e, e);
          fail("Start of hub " + hub.getId() + " threw " + e, e);
        }
      }
    });
  }
      
  private void clearAllListeners() {
    invokeInEveryVM(GatewayDUnitTest.class, "clearListeners");
  }

  protected static void clearListeners() {
    wanRegionListener.clear();
    wanTxListener.clear();
    dsRegionListener.clear();
  }

  private void vmCreateGateway(VM vm, 
                               final String dsName,
                               final int whichvm, 
                               final int hubPortLocal,
                               final String dsNameRemote, 
                               final String hostNameRemote, 
                               final int[] hubPortRemotes,
                               final boolean enablePersistence) 
                        throws Exception {

    vm.invoke(new CacheSerializableRunnable("vmCreateGateway") {
      public void run2() throws CacheException {
        String vmName = getSimpleName() + "_" + dsName+ "_vm" + whichvm;

        // create the gateway hub
        String hubName = getSimpleName() + "_"+dsName;
        String gatewayName = getSimpleName() + "_"+dsNameRemote;

        getLogWriter().info("[GatewayDUnitTest] " + vmName + " is creating " +
          hubName + " with gateway to " + gatewayName);
        Cache cache = getCache();
        GatewayHub hub = cache.setGatewayHub(hubName, hubPortLocal);
        Gateway gateway = addGateway(hub, gatewayName);

        // create the endpoint for remote DS
        for (int i = 0; i < hubPortRemotes.length; i++) {
          getLogWriter().info("[GatewayDUnitTest] " + vmName +
            " adding endpoint [" + gatewayName + ", " + hostNameRemote + ", " +
            hubPortRemotes[i] + "] to " + gatewayName);
          gateway.addEndpoint(gatewayName+i, hostNameRemote, hubPortRemotes[i]);
        }

        // create the gateway queue
        File d = new File(gatewayName + "_overflow_" + OSProcess.getId());
        getLogWriter().info("[GatewayDUnitTest] " + vmName +
          " creating queue in " + d + " for " + gatewayName);

        GatewayQueueAttributes queueAttributes =
          new GatewayQueueAttributes(d.toString(),
            GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
            GatewayQueueAttributes.DEFAULT_BATCH_SIZE,
            GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL,
            GatewayQueueAttributes.DEFAULT_BATCH_CONFLATION,
            enablePersistence,
            GatewayQueueAttributes.DEFAULT_ENABLE_ROLLING,
            GatewayQueueAttributes.DEFAULT_ALERT_THRESHOLD);
        gateway.setQueueAttributes(queueAttributes);
        try {
          hub.start();
        }
        catch (IOException e) {
          getLogWriter().error("Start of hub " + hubName + " threw " + e, e);
          fail("Start of hub " + hubName + " threw " + e, e);
        }
        getLogWriter().info("[GatewayDUnitTest] " + vmName + " has created " +
          hubName + " with gateway to " + gatewayName);
      }
    });
  }
  
  protected static void assertWanListener(Object key, int type) {
    wanRegionListener.waitWhileNoEvents(60000);
    assertTrue(!wanRegionListener.events.isEmpty());
    EventWrapper wrapper =
      (EventWrapper) wanRegionListener.events.removeFirst();
    assertNotNull("Event key is null", wrapper.key);
    assertEquals("Event key not equal", key, wrapper.key);
    assertEquals("Event type not equal", TYPE_DESTROY, wrapper.type);
  }

  protected static void assertWanListener(Object key, Object val, Object arg, int type) {
    wanRegionListener.waitWhileNoEvents(60000);
    assertTrue(!wanRegionListener.events.isEmpty());
    EventWrapper wrapper =
      (EventWrapper) wanRegionListener.events.removeFirst();
    getLogWriter().info("processing wrapper: " + wrapper);
    assertNotNull("Event key is null", wrapper.key);
    assertEquals("Event key not equal", key, wrapper.key);
    assertEquals("Event type not equal", type, wrapper.type);
    if (wrapper.type != TYPE_DESTROY) {
      assertNotNull("Event val is null", wrapper.val);
      assertEquals("Event val not equal", val, wrapper.val);
      assertEquals("Event arg not equal", arg, wrapper.arg);
    }
  }

  protected static void assertTxWanListener(Object key, Object val, int type) {
    wanTxListener.waitWhileNoEvents(60000);
    assertTrue(!wanTxListener.events.isEmpty());
    EventWrapper wrapper =
      (EventWrapper) wanTxListener.events.removeFirst();
    assertNotNull("Event key is null", wrapper.key);
    assertNotNull("Event val is null", wrapper.val);
    assertEquals("Event key not equal", key, wrapper.key);
    assertEquals("Event val not equal", val, wrapper.val);
    assertEquals("Event arg not equal", null, wrapper.arg);
    assertEquals("Event type not equal", type, wrapper.type);
  }
  
  protected static void assertWanListenerNow(Object key, Object val, Object arg, int type) {
    assertTrue(!wanRegionListener.events.isEmpty());
    EventWrapper wrapper =
      (EventWrapper) wanRegionListener.events.removeFirst();
    assertNotNull("Event key is null", wrapper.key);
    assertNotNull("Event val is null", wrapper.val);
    assertEquals("Event key not equal", key, wrapper.key);
    assertEquals("Event val not equal", val, wrapper.val);
    assertEquals("Event arg not equal", arg, wrapper.arg);
    assertEquals("Event type not equal", type, wrapper.type);
  }

  private static final String KEY_SLEEP = "KEY_SLEEP"; // not effective for WAN
  private static final String KEY_WAIT = "KEY_WAIT";
  private static final String KEY_DISCONNECT = "KEY_DISCONNECT";

  protected final static int TYPE_CREATE = 0;
  protected final static int TYPE_UPDATE = 1;
  protected final static int TYPE_INVALIDATE = 2;
  protected final static int TYPE_DESTROY = 3;

//  private static Integer TYPE_CREATE_INTEGER = new Integer(TYPE_CREATE);
//  private static Integer TYPE_UPDATE_INTEGER = new Integer(TYPE_UPDATE);
//  private static Integer TYPE_INVALIDATE_INTEGER = new Integer(TYPE_INVALIDATE);
//  private static Integer TYPE_DESTROY_INTEGER = new Integer(TYPE_DESTROY);

  private static class EventWrapper {
    public final Object key;
    public final Object val;
    public final Object arg;
    public final  int type;
    public EventWrapper(Object key, Object val, Object arg, int type) {
      this.key = key;
      this.val = val;
      this.arg = arg;
      this.type = type;
    }
    public String toString() {
      return "EventWrapper: key=" + key + ", val=" + val + ", arg=" + arg + ", type=" + type;
    }
  }

  protected static class ControlListener extends CacheListenerAdapter {
    public final LinkedList events = new LinkedList();
    public final Object CONTROL_LOCK = new Object();

    public void clear() {
      synchronized(this.CONTROL_LOCK) {
        events.clear();
      }
    }

    public boolean waitWhileNoEvents(long sleepMs) {
      synchronized(this.CONTROL_LOCK) {
        try {
          if (this.events.isEmpty()) {
            this.CONTROL_LOCK.wait(sleepMs);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !this.events.isEmpty();
      }
    }
    
    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount) {
      long maxMillis = System.currentTimeMillis() + sleepMs;
      synchronized(this.CONTROL_LOCK) {
        try {
          while (this.events.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            this.CONTROL_LOCK.wait(waitMillis);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !this.events.isEmpty();
      }
    }

    public void afterCreate(EntryEvent event) {
      getLogWriter().info(event.getRegion().getName() + " afterCreate " + event);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(
          event.getKey(), event.getNewValue(),
          event.getCallbackArgument(), TYPE_CREATE));
        //this.eventTypes.add(TYPE_CREATE_INTEGER);
        this.CONTROL_LOCK.notifyAll();
      }
      processEvent(event);
    }
    public void afterUpdate(EntryEvent event) {
      getLogWriter().info(event.getRegion().getName() + " afterUpdate " + event);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(
          event.getKey(), event.getNewValue(),
          event.getCallbackArgument(), TYPE_UPDATE));
        this.CONTROL_LOCK.notifyAll();
      }
      processEvent(event);
    }
    public void afterInvalidate(EntryEvent event) {
      getLogWriter().info(event.getRegion().getName() + " afterInvalidate " + event);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(
          event.getKey(), event.getNewValue(),
          event.getCallbackArgument(), TYPE_INVALIDATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }
    public void afterDestroy(EntryEvent event) {
      getLogWriter().info(event.getRegion().getName() + " afterDestroy " + event);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(
          event.getKey(), event.getNewValue(),
          event.getCallbackArgument(), TYPE_DESTROY));
        this.CONTROL_LOCK.notifyAll();
      }
    }
    private void processEvent(EntryEvent event) {
      if (event.getKey().equals(KEY_SLEEP)) {
        processSleep(event);
      }
      else if (event.getKey().equals(KEY_WAIT)) {
        processWait(event);
      }
      else if (event.getKey().equals(KEY_DISCONNECT)) {
        processDisconnect(event);
      }
    }
    private void processSleep(EntryEvent event) {
      int sleepMs = ((Integer)event.getNewValue()).intValue();
      getLogWriter().info("[processSleep] sleeping for " + sleepMs);
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException ignore) {
        fail("interrupted");
      }
    }
    private void processWait(EntryEvent event) {
      int sleepMs = ((Integer)event.getNewValue()).intValue();
      getLogWriter().info("[processWait] waiting for " + sleepMs);
      synchronized(this.CONTROL_LOCK) {
        try {
          this.CONTROL_LOCK.wait(sleepMs);
        } catch (InterruptedException ignore) {
          fail("interrupted");
        }
      }
    }
    private void processDisconnect(EntryEvent event) {
      getLogWriter().info("[processDisconnect] disconnecting");
      disconnectFromDS();
    }
  }

  protected static class TxControlListener extends TransactionListenerAdapter {
    public final LinkedList events = new LinkedList();
    public final Object CONTROL_LOCK = new Object();

    public void clear() {
      synchronized(this.CONTROL_LOCK) {
        events.clear();
      }
    }

    public boolean waitWhileNoEvents(long sleepMs) {
      synchronized(this.CONTROL_LOCK) {
        try {
          if (this.events.isEmpty()) {
            this.CONTROL_LOCK.wait(sleepMs);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !this.events.isEmpty();
      }
    }
    
    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount) {
      long maxMillis = System.currentTimeMillis() + sleepMs;
      synchronized(this.CONTROL_LOCK) {
        try {
          while (this.events.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            this.CONTROL_LOCK.wait(waitMillis);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !this.events.isEmpty();
      }
    }

    public void afterCommit(TransactionEvent event) {
      getLogWriter().info("afterCommit event=" + event);
      Iterator it = event.getEvents().iterator();
      while (it.hasNext()) {
        EntryEvent ee = (EntryEvent)it.next();
        synchronized(this.CONTROL_LOCK) {
          int typeCode = 4/*unknown*/;
          if (ee.getOperation().isCreate()) {
            typeCode = TYPE_CREATE;
          } else if (ee.getOperation().isUpdate()) {
            typeCode = TYPE_UPDATE;
          } else if (ee.getOperation().isInvalidate()) {
            typeCode = TYPE_INVALIDATE;
          } else if (ee.getOperation().isDestroy()) {
            typeCode = TYPE_DESTROY;
          }
          this.events.add(new EventWrapper(
                                           ee.getKey(), ee.getNewValue(),
                                           ee.getCallbackArgument(), typeCode));
          this.CONTROL_LOCK.notifyAll();
        }
        processEvent(ee);
      }
    }

    private void processEvent(EntryEvent event) {
      if (event.getKey().equals(KEY_SLEEP)) {
        processSleep(event);
      }
      else if (event.getKey().equals(KEY_WAIT)) {
        processWait(event);
      }
      else if (event.getKey().equals(KEY_DISCONNECT)) {
        processDisconnect(event);
      }
    }
    private void processSleep(EntryEvent event) {
      int sleepMs = ((Integer)event.getNewValue()).intValue();
      getLogWriter().info("[processSleep] sleeping for " + sleepMs);
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException ignore) {
        fail("interrupted");
      }
    }
    private void processWait(EntryEvent event) {
      int sleepMs = ((Integer)event.getNewValue()).intValue();
      getLogWriter().info("[processWait] waiting for " + sleepMs);
      synchronized(this.CONTROL_LOCK) {
        try {
          this.CONTROL_LOCK.wait(sleepMs);
        } catch (InterruptedException ignore) {
          fail("interrupted");
        }
      }
    }
    private void processDisconnect(EntryEvent event) {
      getLogWriter().info("[processDisconnect] disconnecting");
      disconnectFromDS();
    }
  }
  
  protected Gateway addGateway(GatewayHub hub, String gatewayName) {
    return hub.addGateway(gatewayName);
  }

  private String getSimpleName() {
    return getClass().getSimpleName();
  }
}
