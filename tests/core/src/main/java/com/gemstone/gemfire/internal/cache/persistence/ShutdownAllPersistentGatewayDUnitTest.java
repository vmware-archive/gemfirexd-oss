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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserver;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * @author xzhou
 *
 */
public class ShutdownAllPersistentGatewayDUnitTest extends PersistentGatewayDUnitTest {

  //This must be bigger than the dunit ack-wait-threshold for the revoke
  //tests. The command line is setting the ack-wait-threshold to be 
  //60 seconds.
  private static final long MAX_WAIT = 70000;
  private static final int NUM_KEYS = 1000;
  final String expectedExceptions = CacheClosedException.class.getName();

  
  /**
   * @param name
   */
  public ShutdownAllPersistentGatewayDUnitTest(String name) {
    super(name);
  }

  public void testWaitForLatestMemberGateway() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int destinationPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentGateway(vm0, destinationPort);
    getLogWriter().info("Creating region in VM1");
    createPersistentGateway(vm1, destinationPort);
    
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for(int i =0; i < 3; i++) {
          region.put(i, "a" + i);
        }
      }
    });
    
    getLogWriter().info("closing region in vm0");
    closeCache(vm0);

    //Workaround for bug 41572
    Thread.sleep(5000);
    
    vm1.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for(int i =3; i < 5; i++) {
          region.put(i, "a" + i);
        }
      }
    });
    
    getLogWriter().info("closing region in vm1");
    closeCache(vm1);
    
    
    //This ought to wait for VM1 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentGatewayAsync(vm0, destinationPort);
    
    waitForBlockedInitialization(vm0);
    
    assertTrue(future.isAlive());
    
    getLogWriter().info("shutdown all vm0");
    // expect shutdownall will timeout, because vm0 is hang
    shutDownAllMembers(vm0, 1, MAX_WAIT);
    
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("gatewayhub hang after " + MAX_WAIT);
    }
  }

  private AsyncInvocation createPRAtOtherSideAsync(VM vm, final String locatorString, final boolean isServerLocator) {
    SerializableRunnable the_runnable = createPRAtOtherSide(locatorString, isServerLocator);
    return vm.invokeAsync(the_runnable);
  }
  
  private void createPRAtOtherSideSync(VM vm, final String locatorString, final boolean isServerLocator) {
    SerializableRunnable the_runnable = createPRAtOtherSide(locatorString, isServerLocator);
    vm.invoke(the_runnable);
  }
  
  private SerializableRunnable createPRAtOtherSide(final String locatorString, final boolean isServerLocator) {
    SerializableRunnable createPRAtOtherSide = new SerializableRunnable("Create gateway region with "+locatorString) {
      public void run() {
        final Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        if (isServerLocator) {
          props.put("start-locator", locatorString);
          props.put("locators", "");
        } else {
          props.put("locators", locatorString);
        }
        disconnectFromDS();
        waitForCriterion(new WaitCriterion() {

          public String description() {
            return "wait for the peer-locator to start";
          }

          public boolean done() {
            try {
              DistributedSystem ds = getSystem(props);
              return ds != null;
            } catch (Exception e) {
              return false;
            }
          }
        }, 10000, 100, true);
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(getDiskDirs());
        dsf.setMaxOplogSize(1);
        DiskStore store = dsf.create(REGION_NAME);

        RegionFactory rf = new RegionFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        rf.setPartitionAttributes(paf.create());
        rf.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        rf.setEnableGateway(true);
        rf.setDiskStoreName(store.getName());
        final Region region = rf.create(REGION_NAME);
        assertTrue(InternalLocator.isDedicatedLocator()==false);
        
//        this code can be used to prove gateway cannot work on subregion
//        AttributesFactory factory = new AttributesFactory();
//        PartitionAttributesFactory paf = new PartitionAttributesFactory();
//        factory.setPartitionAttributes(paf.create());
//        factory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
//        factory.setDiskStoreName(store.getName());
//        factory.setEnableGateway(true);
//        createRootRegion(new AttributesFactory().create());
//        final Region region =  createRegion(REGION_NAME, factory.create());
      }
    };
    return createPRAtOtherSide;
  }
  
  // new shutdownall will close all gatewayhubs before shutdownall
  public void testShutdownAllGatewayHubs() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int destinationPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm2);
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    
    getLogWriter().info("mcastPort is "+mcastPort+", destinationPort is "+destinationPort);
    getLogWriter().info("Creating region in VM0");
    createPersistentGateway(vm0, destinationPort);

    //Create the other side of the gateway, to make sure we get all of the updates
    final String locatorString1 = getServerHostName(host) + "["+locatorPort+"]";
    final String locatorString2 = getServerHostName(host) + "["+locatorPort+"],peer=true,server=false";
    vm1.invoke(addExceptionTag1(expectedExceptions));
    vm2.invoke(addExceptionTag1(expectedExceptions));
    
    SerializableRunnable createGateWayHubAtOtherSide = new SerializableRunnable("Create gateway region") {
      public void run() {
        GatewayHub hub = getCache().addGatewayHub("g1", destinationPort);
        Gateway gateway = hub.addGateway("h1");
        gateway.setQueueAttributes(new GatewayQueueAttributes(null, 5, 1, 100, false, false, 60000));
        try {
          hub.start();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    createPRAtOtherSideSync(vm2, locatorString2, true);
    createPRAtOtherSideSync(vm1, locatorString1, false);
    vm1.invoke(createGateWayHubAtOtherSide);

    AsyncInvocation vm0_future = vm0.invokeAsync(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(REGION_NAME);
        for(int i =0; i < NUM_KEYS; i++) {
          region.put(i, "a" + i);
        }
      }
    });

//    vm2.invoke(new SerializableRunnable() {
//      public void run() {
//        Properties config = new Properties();
//        config.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastPort));
//        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
//        getSystem(config);
//        Cache cache = getCache();
//      }
//    });
    pause(2000);
    shutDownAllMembers(vm2, 2, MAX_WAIT);
    
    // now restart vm1 with gatewayHub
    getLogWriter().info("restart in VM1");
    AsyncInvocation vm2_future = createPRAtOtherSideAsync(vm2, locatorString2, true);
    createPRAtOtherSideSync(vm1, locatorString1, false);

    vm2_future.join(MAX_WAIT);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        final Region region = cache.getRegion(REGION_NAME);

        cache.getLogger().info("vm1's region size before restart gatewayhub is "+region.size());
      }
    });
    vm1.invoke(createGateWayHubAtOtherSide);

    // wait for vm0 to finish its work
    vm0_future.join(MAX_WAIT);
    vm0.invoke(new SerializableRunnable() {
      public void run() {
        Region region = getRootRegion(REGION_NAME);
        assertEquals(NUM_KEYS, region.size());
      }
    });

    // verify the other side (vm1)'s entries received from gateway
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        final Region region = cache.getRegion(REGION_NAME);

        cache.getLogger().info("vm1's region size after restart gatewayhub is "+region.size());
        waitForCriterion(new WaitCriterion() {
          public boolean done() {
            Object lastvalue = region.get(NUM_KEYS - 1);
            if (lastvalue != null && lastvalue.equals("a" + (NUM_KEYS-1))) {
              region.getCache().getLogger().info("Last key has arrived, its value is "+lastvalue+", end of wait.");
              return true;
            } else return (region.size() == NUM_KEYS); 
          }
          
          public String description() {
            return "Waiting for destination region to reach size: " + NUM_KEYS+", current is " + region.size();
          }
        }, MAX_WAIT, 100, true);
        assertEquals(NUM_KEYS, region.size());
        for(int i = 0; i < 5; i++) {
          assertEquals("a" + (NUM_KEYS-1-i), region.get(NUM_KEYS - 1 - i));
        }
      }
    });
    vm1.invoke(removeExceptionTag1(expectedExceptions));
    vm2.invoke(removeExceptionTag1(expectedExceptions));
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
    deleteStateFile(locatorPort);
  }

  private void deleteStateFile(int port) {
    File stateFile = new File("locator"+port+"state.dat");
    if (stateFile.exists()) {
      stateFile.delete();
    }
  }
  

  private void waitForBlockedInitialization(VM vm) {
    vm.invoke(new SerializableRunnable() {

      public void run() {
        waitForCriterion(new WaitCriterion() {

          public String description() {
            return "Waiting to blocked waiting for anothe persistent member to come online";
          }
          
          public boolean done() {
            GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
            PersistentMemberManager mm = cache.getPersistentMemberManager();
            Map<String, Set<PersistentMemberID>> regions = mm.getWaitingRegions();
            boolean done = !regions.isEmpty();
            return done;
          }
          
        }, MAX_WAIT, 100, true);
        
      }
      
    });
  }

  private void shutDownAllMembers(VM vm, final int expnum, final long timeout) {
    vm.invoke(new SerializableRunnable("Shutdown all the members") {

      public void run() {
        DistributedSystemConfig config;
        AdminDistributedSystemImpl adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = (AdminDistributedSystemImpl)AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Set members = adminDS.shutDownAllMembers(timeout); 
          int num = members==null?0:members.size();
          assertEquals(expnum, num);
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

}
