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
package com.gemstone.gemfire.internal.admin;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.internal.SystemMemberCacheImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache30.BridgeTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * A test to make sure that client stats
 * are sent from clients to servers.
 * @author dsmith
 *
 */
public class ClientStatsManagerDUnitTest extends AdminDUnitTestCase {

  private static final long MAX_WAIT = 60 * 1000;

  public ClientStatsManagerDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Test to make sure that the server receives the stats from the client.
   * @throws Exception
   */
  public void testStats() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
    
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    
    startBridgeServer(vm0, serverPort);
    
    vm1.invoke(new SerializableRunnable("SetupBridgeClient") {
      public void run() {
        Properties props = new Properties();
        props.setProperty("locators", "");
        getSystem(props);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        synchronized (ClientStatsManager.class) {
          ClientStatsManager.lastInitializedCache = null;
        }
        BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), new int[] {serverPort}, false, -1, 1, null, 1000, -1, false, -1,
                                                           1000/*stats every second*/);
        Cache cache = getCache();
        Region region = cache.createRegion(ClientStatsManagerDUnitTest.class.getName(), factory.create());
        region.put("key", "value");
        region.get("key2");
      }
    });
    
    checkForClientStats(1,1);
  }
  
  public void testTwoPools() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
    
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    
    startBridgeServer(vm0, serverPort);
    
    vm1.invoke(new SerializableRunnable("SetupBridgeClient") {
      public void run() {
        Properties props = new Properties();
        props.setProperty("locators", "");
        getSystem(props);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        synchronized (ClientStatsManager.class) {
          ClientStatsManager.lastInitializedCache = null;
        }
        BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), new int[] {serverPort}, false, -1, 1, null, 1000, -1, false, -1,
                                                           1000/*stats every second*/);
        Cache cache = getCache();
        Region region = cache.createRegion(ClientStatsManagerDUnitTest.class.getName(), factory.create());
        region.put("key", "value");
        region.get("key2");
      }
    });
    
    checkForClientStats(1,1);
    
    vm1.invoke(new SerializableRunnable("CreateNewPool") {
      public void run() {
        Cache cache = getCache();
        Region oldRegion = cache.getRegion(ClientStatsManagerDUnitTest.class.getName());
        Pool oldPool = PoolManager.find(oldRegion);
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        synchronized (ClientStatsManager.class) {
          ClientStatsManager.lastInitializedCache = null;
        }
        BridgeTestCase.configureConnectionPoolWithName(factory,
            getServerHostName(host), new int[] { serverPort }, false, -1, 1,
            null, "pool2", 1000, -1, false, -1, 1000/* stats every second */);
        Region newRegion = cache.createRegion(ClientStatsManagerDUnitTest.class.getName() + "-2", factory.create());
        
        oldRegion.close();
        oldPool.destroy();
        newRegion.put("key", "value");
        newRegion.get("key2");
        
      }
    });
    
    checkForClientStats(2,2);
  }

  private void checkForClientStats(final int puts, final int gets) 
      throws Exception, AdminException, InterruptedException {
  //Wait for the server to get a stat update from the client
    
    final AdminDistributedSystem ds = getAdminDistributedSystem();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return ds.getSystemMemberApplications().length != 0;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return "Expected some system member applications";
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_WAIT, 200, true);
    SystemMember[] members =  ds.getSystemMemberApplications();
    final SystemMemberCacheImpl serverCache = (SystemMemberCacheImpl) members[0].getCache();
    
    ev = new WaitCriterion() {
      public boolean done() {
        GemFireMemberStatus status = serverCache.getSnapshot();
        if (status.getClientHealthStats().size() == 0) {
          return false;
        }
        try {
          //Make sure that all of the stats are accurate.
          Map clientHealthStats = status.getClientHealthStats();
          Assert.assertEquals(1, clientHealthStats.size());
          ClientHealthStats stats = (ClientHealthStats) clientHealthStats.values().iterator().next();
          Assert.assertEquals(puts,stats.getNumOfPuts());
          Assert.assertEquals(gets,stats.getNumOfGets());
          return true;
        } 
        catch(AssertionFailedError e) {
          return false;
        }
      }
      public String description() {
        GemFireMemberStatus status = serverCache.getSnapshot();
        if (status.getClientHealthStats().size() == 0) {
          return "expected client health stats to exist";
        }
        Map clientHealthStats = status.getClientHealthStats();
        if (clientHealthStats.size() != 1) {
          return "expected one client health stats";
        }
        ClientHealthStats stats = (ClientHealthStats) clientHealthStats.values().iterator().next();
        return "Expected puts to be " + puts + " but it was " + stats.getNumOfPuts() + " expected gets to be " + gets + " but it was " + stats.getNumOfGets();
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAX_WAIT, 200, true);
  }

  private void startBridgeServer(VM vm, final int serverPort) {
    vm.invoke(new SerializableCallable("SetupBridgeServer") {
      public Object call() throws IOException {
        Cache cache = getCache();
        BridgeServer bridge = cache.addBridgeServer();
        bridge.setPort(serverPort);
        bridge.start();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        cache.createRegion(ClientStatsManagerDUnitTest.class.getName(), factory.create());
        cache.createRegion(ClientStatsManagerDUnitTest.class.getName() + "-2", factory.create());
        return null;
      }
    });
  }
  
}
