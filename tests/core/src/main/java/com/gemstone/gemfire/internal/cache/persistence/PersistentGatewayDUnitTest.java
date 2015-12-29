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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class PersistentGatewayDUnitTest extends CacheTestCase {

  protected static final String REGION_NAME = "REGION_NAME";
  //This must be bigger than the dunit ack-wait-threshold for the revoke
  //tests. The command line is setting the ack-wait-threshold to be 
  //60 seconds.
  private static final long MAX_WAIT = 70000;
  
  /**
   * @param name
   */
  public PersistentGatewayDUnitTest(String name) {
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
    
    getLogWriter().info("Creating region in VM1");
    createPersistentGateway(vm1, destinationPort);
    
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }
    
    //close the cache in vm1, to make sure that vm0 has received all of the updates.
    closeCache(vm1);
    
    //Create the other side of the gateway, to make sure we get all of the updates
    SerializableRunnable createOtherSide = new SerializableRunnable("Create gateway region") {
      public void run() {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        DistributedSystem ds = getSystem(props);
        Cache cache = getCache();
        RegionFactory rf = new RegionFactory();
        rf.setDataPolicy(DataPolicy.REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setEnableGateway(true);
        final Region region = rf.create(REGION_NAME);
        GatewayHub hub = cache.addGatewayHub("h1", destinationPort);
        Gateway gateway = hub.addGateway("g1");
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(getDiskDirs());
        dsf.setMaxOplogSize(1);
        DiskStore store = dsf.create(REGION_NAME);
        gateway.setQueueAttributes(new GatewayQueueAttributes(store.getName(), 5, 1, 100, false, true, 60000));
        try {
          hub.start();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        
        waitForCriterion(new WaitCriterion() {
          
          public boolean done() {
            return region.size() == 5;
          }
          
          public String description() {
            return "Waiting for destination region to contain entries. Current keys " + region.keySet();
          }
        }, MAX_WAIT, 100, true);
        
        for(int i =0; i < 5; i++) {
          assertEquals("a" + i, region.get(i));
        }
      }
    };
    
    vm2.invoke(createOtherSide);
  }
  
  protected void closeCache(final VM vm) {
    SerializableRunnable closeRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };
    vm.invoke(closeRegion);
  }
  
  protected void createPersistentGateway(VM vm, int destinationPort) throws Throwable {
    AsyncInvocation future = createPersistentGatewayAsync(vm, destinationPort);
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
  }

  protected AsyncInvocation createPersistentGatewayAsync(final VM vm, final int destinationPort) {
    SerializableRunnable createRegion = new SerializableRunnable("Create gateway region") {
      public void run() {
        Cache cache = getCache();
        RegionFactory rf = new RegionFactory();
        rf.setDataPolicy(DataPolicy.REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setEnableGateway(true);
        rf.create(REGION_NAME);
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        GatewayHub hub = cache.addGatewayHub("h1", port);
        Gateway gateway = hub.addGateway("g1");
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(getDiskDirs());
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(REGION_NAME);
        gateway.setQueueAttributes(new GatewayQueueAttributes(ds.getName(), 5, 1, 100, false, true, 60000));
        try {
          gateway.addEndpoint("end1", InetAddress.getLocalHost().getHostAddress(), destinationPort);
          hub.start();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    return vm.invokeAsync(createRegion);
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
}
