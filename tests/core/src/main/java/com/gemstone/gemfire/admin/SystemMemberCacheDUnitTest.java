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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbeAdapter;
import com.gemstone.gemfire.cache.server.ServerMetrics;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import dunit.*;

import java.io.Serializable;
import java.util.*;

/**
 * Tests the functionality of the {@link SystemMemberCache}
 * administration API.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class SystemMemberCacheDUnitTest extends AdminDUnitTestCase {

  /**
   * Creates a new <code>SystemMemberCacheDUnitTest</code>
   */
  public SystemMemberCacheDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Tests getting and setting attributes of a cache using the admin
   * AP.I
   */
  public void testCacheAttributes() throws Exception {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    final String testName = this.getName();

    vm.invoke(new SerializableRunnable() {
        public void run() {
          Properties props = getDistributedSystemProperties();
          props.setProperty(DistributionConfig.NAME_NAME, testName);
          getSystem(props);
        }
      });
    pause(2 * 1000);

    AdminDistributedSystem system = this.tcSystem;
    SystemMember[] members = system.getSystemMemberApplications();
    if (members.length != 1) {
      StringBuffer sb = new StringBuffer();
      sb.append("Expected 1 member, got " + members.length + ": ");
      for (int i = 0; i < members.length; i++) {
        SystemMember member = members[i];
        sb.append(member.getName());
        sb.append(" ");
      }

      fail(sb.toString());
    }

    SystemMemberCache cache = members[0].getCache();
    assertNull(cache);

    final int oldLockLease = 27;
    final int oldLockTimeout = 28;
    final int oldSearchTimeout = 29;
    
    vm.invoke(new CacheSerializableRunnable("Create cache") {
        public void run2() throws CacheException {
          System.setProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
          try {
          Cache cache2 = CacheFactory.create(getSystem());
          cache2.setLockLease(oldLockLease);
          cache2.setLockTimeout(oldLockTimeout);
          cache2.setSearchTimeout(oldSearchTimeout);
          } finally {
            System.clearProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
          }
        }
      });

    cache = members[0].getCache();
    assertNotNull(cache);
    
    assertEquals(testName, cache.getName());
    assertEquals(oldLockLease, cache.getLockLease());
    assertEquals(oldLockTimeout, cache.getLockTimeout());
    assertEquals(oldSearchTimeout, cache.getSearchTimeout());

    final int newLockLease = 27;
    final int newLockTimeout = 28;
    final int newSearchTimeout = 29;

    cache.setLockLease(newLockLease);
    cache.setLockTimeout(newLockTimeout);
    cache.setSearchTimeout(newSearchTimeout);

    vm.invoke(new CacheSerializableRunnable("Verify cache config") {
        public void run2() throws CacheException {
          Cache cache2 = CacheFactory.getAnyInstance();
          assertEquals(newLockLease, cache2.getLockLease());
          assertEquals(newLockTimeout, cache2.getLockTimeout());
          assertEquals(newSearchTimeout, cache2.getSearchTimeout());
          cache2.close();
        }
      });

    //Thread.sleep(500);

    cache.refresh();
    assertTrue(cache.isClosed());
  }

  /**
   * Tests getting root regions
   */
  public void testRootRegions() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

//    long start = System.currentTimeMillis();
    vm.invoke(new CacheSerializableRunnable("Create cache") {
        public void run2() throws CacheException {
          System.setProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
          try {
            CacheFactory.create(getSystem());
          } finally {
            System.clearProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
          }
        }
      });
    pause(250);

    for(int i=0;i< NUM_RETRY ; i++) {
      Thread.sleep(RETRY_INTERVAL);
      
      SystemMember[] members = system.getSystemMemberApplications();
      if(members.length > 0) {
       break; 
      }   
    }
    
    assertTrue(system.getSystemMemberApplications().length > 0);
    
    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    assertNotNull(cache);
    assertTrue(cache.getRootRegionNames().isEmpty());

    final String root1 = "root1";
    final String root2 = "root2";
    final String root3 = "root3";

    vm.invoke(new CacheSerializableRunnable("Create roots") {
        public void run2() throws CacheException {
          Cache cache2 = CacheFactory.getAnyInstance();
          RegionAttributes attrs =
            new AttributesFactory().create();
          cache2.createRegion(root1, attrs);
          cache2.createRegion(root2, attrs);
          cache2.createRegion(root3, attrs);
        }
      });

    cache.refresh();
    SortedSet roots = new TreeSet(cache.getRootRegionNames());
    assertEquals(3, roots.size());

    for (Iterator iter = roots.iterator(); iter.hasNext(); ) {
      String name = (String) iter.next();
      assertNotNull(cache.getRegion(name));
    }

    cache.refresh();
    int oldUpTime = cache.getUpTime();
    pause(1000);
    cache.refresh();
    assertTrue(cache.getUpTime() > oldUpTime);

    vm.invoke(new CacheSerializableRunnable("Verify cache config") {
        public void run2() throws CacheException {
          Cache cache2 = CacheFactory.getAnyInstance();
          cache2.close();
        }
      });

    cache.refresh();
    assertTrue(cache.isClosed());
  }

  /**
   * Tests adding a bridge server in a remote VM.
   *
   * @since 4.0
   */
  public void testAddBridgeServer() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    final AdminDistributedSystem system = this.tcSystem;

    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          assertEquals(0, cache.getBridgeServers().size());
        }
      });

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return system.getSystemMemberApplications().length != 0;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return "system member applications remained empty";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    assertNotNull(cache);
    assertEquals(0, cache.getBridgeServers().length);

    SystemMemberBridgeServer bridge = cache.addBridgeServer();
    assertNotNull(bridge);
    assertFalse(bridge.isRunning());
    assertEquals(BridgeServer.DEFAULT_PORT, bridge.getPort());
    assertEquals(BridgeServer.DEFAULT_BIND_ADDRESS, bridge.getBindAddress());
    assertEquals(BridgeServer.DEFAULT_HOSTNAME_FOR_CLIENTS, bridge.getHostnameForClients());
    assertEquals(BridgeServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION, bridge.getNotifyBySubscription());
    assertEquals(BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE, bridge.getSocketBufferSize());
    assertEquals(BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, bridge.getMaximumTimeBetweenPings());
    assertEquals(BridgeServer.DEFAULT_MAX_CONNECTIONS, bridge.getMaxConnections());
    assertEquals(BridgeServer.DEFAULT_MAX_THREADS, bridge.getMaxThreads());
    assertEquals(BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, bridge.getMaximumMessageCount());
    assertEquals(BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE, bridge.getMessageTimeToLive());
    assertEquals(Arrays.asList(BridgeServer.DEFAULT_GROUPS), Arrays.asList(bridge.getGroups()));
    assertEquals(BridgeServer.DEFAULT_LOAD_POLL_INTERVAL, bridge.getLoadPollInterval());
    assertEquals(BridgeServer.DEFAULT_LOAD_PROBE.toString(), bridge.getLoadProbe());

    cache.refresh();
    assertEquals(1, cache.getBridgeServers().length);

    vm.invoke(new CacheSerializableRunnable("Add another bridge server") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          cache2.addBridgeServer();
          assertEquals(2, cache2.getBridgeServers().size());
        }
      });

    cache.refresh();
    assertEquals(2, cache.getBridgeServers().length);

    vm.invoke(new CacheSerializableRunnable("Close Cache") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          cache2.close();
        }
      });
  }
  /**
   * @since 5.7
   */
  public void testAddCacheServer() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          assertEquals(0, cache.getCacheServers().size());
        }
      });

    {
      int waits = 0;
      while (waits < 20 && system.getSystemMemberApplications().length == 0) {
        waits++;
        pause(100);
      }
    }

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    assertNotNull(cache);
    assertEquals(0, cache.getCacheServers().length);

    SystemMemberCacheServer bridge = cache.addCacheServer();
    assertNotNull(bridge);
    assertFalse(bridge.isRunning());
    assertEquals(CacheServer.DEFAULT_PORT, bridge.getPort());
    assertEquals(CacheServer.DEFAULT_BIND_ADDRESS, bridge.getBindAddress());
    assertEquals(CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS, bridge.getHostnameForClients());
    //assertEquals(CacheServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION, bridge.getNotifyBySubscription());
    assertEquals(CacheServer.DEFAULT_SOCKET_BUFFER_SIZE, bridge.getSocketBufferSize());
    assertEquals(CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, bridge.getMaximumTimeBetweenPings());
    assertEquals(CacheServer.DEFAULT_MAX_CONNECTIONS, bridge.getMaxConnections());
    assertEquals(CacheServer.DEFAULT_MAX_THREADS, bridge.getMaxThreads());
    assertEquals(CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, bridge.getMaximumMessageCount());
    assertEquals(CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, bridge.getMessageTimeToLive());
    assertEquals(Arrays.asList(CacheServer.DEFAULT_GROUPS), Arrays.asList(bridge.getGroups()));
    assertEquals(CacheServer.DEFAULT_LOAD_POLL_INTERVAL, bridge.getLoadPollInterval());
    assertEquals(CacheServer.DEFAULT_LOAD_PROBE.toString(), bridge.getLoadProbe());

    cache.refresh();
    assertEquals(1, cache.getCacheServers().length);

    vm.invoke(new CacheSerializableRunnable("Add another bridge server") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          cache2.addCacheServer();
          assertEquals(2, cache2.getCacheServers().size());
        }
      });

    cache.refresh();
    assertEquals(2, cache.getCacheServers().length);

    vm.invoke(new CacheSerializableRunnable("Close Cache") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          cache2.close();
        }
      });
  }

  /**
   * Tests starting and stopping a bridge server in a remote VM
   *
   * @since 4.0
   */
  public void testStartStopBridgeServer() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          assertEquals(0, cache.getBridgeServers().size());
        }
      });
    pause(5 * 1000);

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    final SystemMemberBridgeServer bridge = cache.addBridgeServer();

    final int port =
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    bridge.setPort(port);
    
    bridge.setBindAddress(java.net.InetAddress.getLocalHost().getHostName());
    bridge.setHostnameForClients("bogusClientHostName");
    bridge.setNotifyBySubscription(BridgeServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION);
    bridge.setSocketBufferSize(7777);
    bridge.setMaximumTimeBetweenPings(666);
    bridge.setMaxConnections(555);
    bridge.setMaxThreads(444);
    bridge.setMaximumMessageCount(333);
    bridge.setMessageTimeToLive(222);
    bridge.setGroups(new String[]{"myServerGroup1", "myServerGroup2"});
    bridge.setLoadPollInterval(8888);
    if(!isJMX()) {
      bridge.setLoadProbe(new MyLoadProbe("myLoadProbe"));
    }
    bridge.start();
    bridge.refresh();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        if (bridge.isRunning()) return true;
        bridge.refresh();
        return false;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
    assertTrue(bridge.isRunning());
    assertEquals(port, bridge.getPort());
    assertEquals(java.net.InetAddress.getLocalHost().getHostName(), bridge.getBindAddress());
    assertEquals("bogusClientHostName", bridge.getHostnameForClients());
    assertEquals(BridgeServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION, bridge.getNotifyBySubscription());
    assertEquals(7777, bridge.getSocketBufferSize());
    assertEquals(666, bridge.getMaximumTimeBetweenPings());
    assertEquals(555, bridge.getMaxConnections());
    assertEquals(444, bridge.getMaxThreads());
    assertEquals(333, bridge.getMaximumMessageCount());
    assertEquals(222, bridge.getMessageTimeToLive());
    assertEquals(Arrays.asList(new String[]{"myServerGroup1", "myServerGroup2"}), Arrays.asList(bridge.getGroups()));
    assertEquals(8888, bridge.getLoadPollInterval());
    if(!isJMX()) {
      assertEquals("myLoadProbe", bridge.getLoadProbe());
    }
    try {
      bridge.setPort(23);
      fail("expected AdminException");
    } catch (AdminException expected) {
    }
    
    vm.invoke(new CacheSerializableRunnable("Check Bridge Started") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          Collection bridges = cache2.getBridgeServers();
          assertEquals(1, bridges.size());

          BridgeServer bridge2 =
            (BridgeServer) bridges.iterator().next();
          assertTrue(bridge2.isRunning());
          assertEquals(port, bridge2.getPort());
        }
      });
    
    bridge.stop();
    bridge.refresh();
    ev = new WaitCriterion() {
      public boolean done() {
        if (!bridge.isRunning()) return true;
        bridge.refresh();
        return false;
      }
      public String description() {
        return "bridge never started running: " + bridge;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
    assertFalse(bridge.isRunning());

    vm.invoke(new CacheSerializableRunnable("Check Bridge Stopped") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          Collection bridges = cache2.getBridgeServers();
          assertEquals(1, bridges.size());

          BridgeServer bridge2 =
            (BridgeServer) bridges.iterator().next();
          assertFalse(bridge2.isRunning());

          cache2.close();
        }
      });
  }

  /**
   * Tests to make sure that the admin API detects cache servers
   * correctly. 
   *
   * @since 4.0
   */
  public void testIsCacheServer() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          getCache();
        }
      });
    pause(250);

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    assertFalse(cache.isServer());

    vm.invoke(new CacheSerializableRunnable("Mark as Cache Server") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          cache2.setIsServer(true);
          assertTrue(cache2.isServer());
        }
      });

    assertFalse(cache.isServer());
    cache.refresh();
    assertTrue(cache.isServer());

    vm.invoke(new CacheSerializableRunnable("Unmark as Cache Server") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          cache2.setIsServer(false);
          assertFalse(cache2.isServer());
        }
      });

    vm.invoke(new CacheSerializableRunnable("Test getAdminMembers in Cache") {
      public void run2() throws CacheException {
        Cache cache2 = getCache();
        assertEquals(1, cache2.getAdminMembers().size());
      }
    });

    assertTrue(cache.isServer());
    cache.refresh();
    assertFalse(cache.isServer());
  }

  /**
   * Tests that the admin API can create a VM root region in a remote
   * VM. 
   *
   * @since 4.0
   */
  public void testCreateRootRegion() throws Exception {
    if (isJMX()) {
      // Can't create regions with JMX
      return;
    }

    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          getCache();
        }
      });
    pause(250);

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    assertEquals(0, cache.getRootRegionNames().size());
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setKeyConstraint(String.class);

    final RegionAttributes attrs = factory.create();
    final String name = this.getUniqueName();
    
    SystemMemberRegion region = cache.createRegion(name, attrs);
    assertEquals(name, region.getName());
    assertEquals(Scope.LOCAL, region.getScope());
    assertEquals(String.class.getName(), region.getKeyConstraint());

    vm.invoke(new CacheSerializableRunnable("Verify region") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          Region region2 = cache2.getRegion(name);
          assertNotNull(region2);

          RegionAttributes attrs2 = region2.getAttributes();
          assertEquals(name, region2.getName());
          assertEquals(Scope.LOCAL, attrs2.getScope());
          assertEquals(String.class, attrs2.getKeyConstraint());
        }
      });
  }

  /**
   * Tests that the admin API can create a region in a remote VM.
   *
   * @since 4.0
   */
  public void testCreateRegion() throws Exception {
    if (isJMX()) {
      // Can't create regions with JMX
      return;
    }

    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

    final String rootName = this.getUniqueName() + "-ROOT";
    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          cache.createRegion(rootName,
                               factory.create());
        }
      });
    pause(250);

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    Set rootNames = cache.getRootRegionNames();
    assertEquals(1, rootNames.size());
    assertEquals(rootName, rootNames.iterator().next());

    SystemMemberRegion root = cache.getRegion(rootName);
    assertNotNull(root);
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setCacheLoader(new MyCacheLoader());
    factory.setKeyConstraint(String.class);

    final RegionAttributes attrs = factory.create();
    final String name = this.getUniqueName() + "-Subregion";
    
    SystemMemberRegion region = root.createSubregion(name, attrs);
    assertEquals(name, region.getName());
    assertEquals(Scope.LOCAL, region.getScope());
    assertEquals(String.class.getName(), region.getKeyConstraint());

    vm.invoke(new CacheSerializableRunnable("Verify region") {
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          Region root2 = cache2.getRegion(rootName);
          assertNotNull(root2);

          Region region2 = root2.getSubregion(name);
          assertNotNull(region2);

          RegionAttributes attrs2 = region2.getAttributes();
          assertEquals(name, region2.getName());
          assertEquals(Scope.LOCAL, attrs2.getScope());
          assertEquals(String.class, attrs2.getKeyConstraint());
//Commented out due to bug ????          
//          assertEquals(MyCacheLoader.class, attrs2.getCacheLoader().getClass());
        }
      });
  }

  /**
   * Tests that creating a region in a remote VM with invalid
   * attributes throws an exception.
   *
   * @since 4.0
   */
  public void testCreateRegionBadConfiguration() throws Exception {
    if (isJMX()) {
      // Can't create regions with JMX
      return;
    }

    VM vm = Host.getHost(0).getVM(0);

    AdminDistributedSystem system = this.tcSystem;

    final String rootName = this.getUniqueName() + "-ROOT";
    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          cache.createRegion(rootName,
                               factory.create());
        }
      });
    pause(250);

    SystemMemberCache cache =
      system.getSystemMemberApplications()[0].getCache();
    Set rootNames = cache.getRootRegionNames();
    assertEquals(1, rootNames.size());
    assertEquals(rootName, rootNames.iterator().next());

    SystemMemberRegion root = cache.getRegion(rootName);
    assertNotNull(root);
    
    // Parent region is LOCAL scope, subregion must also be LOCAL
    // scope.
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);

    final RegionAttributes attrs = factory.create();
    final String name = this.getUniqueName() + "-Subregion";
    
    try {
      root.createSubregion(name, attrs);
      fail("Should have thrown an AdminException");

    } catch (AdminException ex) {
      assertTrue(ex.getCause() instanceof IllegalStateException);
    }
  }
  
  public static class MyCacheLoader implements CacheLoader {

    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }

    public void close() {
    }
  }
  
  public static class MyLoadProbe extends ServerLoadProbeAdapter implements Serializable {

    private String string;

    public MyLoadProbe(String string) {
      this.string = string;
    }
    public ServerLoad getLoad(ServerMetrics metrics) {
      return new ServerLoad();
    }
    
    public String toString() {
      return string;
    }
  }

}
