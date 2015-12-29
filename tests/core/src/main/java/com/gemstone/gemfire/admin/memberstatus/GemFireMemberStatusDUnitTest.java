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
package com.gemstone.gemfire.admin.memberstatus;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.SystemMemberBridgeServer;
import com.gemstone.gemfire.admin.SystemMemberCache;
import com.gemstone.gemfire.admin.internal.SystemMemberCacheImpl;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests the functionality of the {@link GemFireMemberStatus}
 * object used both in GFMonI and II for core status 
 *
 * @author Harsh Khanna
 * @since 5.7
 */
public class GemFireMemberStatusDUnitTest extends AdminDUnitTestCase
{
  public static final int MEM_VM = 0;
  
  protected SystemMemberCache cache;

  private Set initedVMs;
  
    /**
     * Creates a new <code>GemFireMemberStatusDUnitTest</code>
     */
    public GemFireMemberStatusDUnitTest(String name) {
      super(name);
      initedVMs = new LinkedHashSet();
    }

    ////////  Test Methods

    /**
     * Tests utilities to create a member
     */
    protected VM getMemberVM() {
      Host host = Host.getHost(0);
      return host.getVM(MEM_VM);
    }
    
    /**
     * Tests that status object serialized properly by looking at
     * various attributed
     */
    public void testStatusSerialization() throws Exception {
      // Create Member an Cache
      Host host = Host.getHost(0);
      VM vm = host.getVM(MEM_VM);
      
      final String testName = this.getName();

      vm.invoke(new SerializableRunnable() {
          public void run() {
            Properties props = getDistributedSystemProperties();
            props.setProperty(DistributionConfig.NAME_NAME, testName);
            getSystem(props);
          }
        });
      pause(2 * 1000);
      
      getLogWriter().info("Test: Created DS");
      
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

      getLogWriter().info("Test: Created Member");

      cache = members[0].getCache();
      assertNull(cache);
      vm.invoke(new CacheSerializableRunnable("Create cache") {
          public void run2() throws CacheException {
            CacheFactory.create(getSystem());
          }
        });

      getLogWriter().info("Test: Created Cache");
      cache = members[0].getCache();
      assertNotNull(cache);
      
      // Get the Status Object, verify not null & not a server
      GemFireMemberStatus status = ((SystemMemberCacheImpl)cache).getSnapshot();
      assertNotNull(status);
      assertFalse(status.getIsServer());
    }

    /**
     * Tests that status object serialized properly by looking at
     * various attributes for 1 server, 1 peer
     */
    public void testStatusSerializationForServerAndPeer() throws Exception {
      // Create Member an Cache
      final String testName = this.getName();

      Host host = Host.getHost(0);

      VM vm1 = host.getVM(0);
      vm1.invoke(new SerializableRunnable() {
          public void run() {
            Properties props = getDistributedSystemProperties();
            props.setProperty(DistributionConfig.NAME_NAME, testName+0);
            getSystem(props);
          }
        });
      pause(2 * 1000);

      VM vm2 = host.getVM(1);
      vm2.invoke(new SerializableRunnable() {
          public void run() {
            Properties props = getDistributedSystemProperties();
            props.setProperty(DistributionConfig.NAME_NAME, testName+1);
            getSystem(props);
          }
        });
      pause(2 * 1000);
      
      getLogWriter().info("Test: Created DS in both VMs");
      
      AdminDistributedSystem system = this.tcSystem;
      SystemMember[] members = system.getSystemMemberApplications();
      if (members.length != 2) {
        StringBuffer sb = new StringBuffer();
        sb.append("Expected 2 member, got " + members.length + ": ");
        for (int i = 0; i < members.length; i++) {
          SystemMember member = members[i];
          sb.append(member.getName());
          sb.append(" ");
        }
        fail(sb.toString());
      }

      getLogWriter().info("Test: Created Members");

      SystemMemberCache cache1 = members[0].getCache();
      assertNull(cache1);

      SystemMemberCache cache2 = members[1].getCache();
      assertNull(cache2);

      vm1.invoke(new CacheSerializableRunnable("Create cache") {
          public void run2() throws CacheException {
            CacheFactory.create(getSystem());
          }
        });

      vm2.invoke(new CacheSerializableRunnable("Create cache") {
        public void run2() throws CacheException {
          CacheFactory.create(getSystem());
        }
      });

      getLogWriter().info("Test: Created Caches");

      cache1 = members[0].getCache();
      assertNotNull(cache1);

      cache2 = members[1].getCache();
      assertNotNull(cache2);
      
      // Add bridge server to cache 2
      SystemMemberBridgeServer bridge = cache2.addBridgeServer();
      final int port =
        AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      bridge.setPort(port);
      bridge.start();
      assertEquals(port, bridge.getPort());
      
      // Get the Status Object, verify not null & not a server
      GemFireMemberStatus status = ((SystemMemberCacheImpl)cache2).getSnapshot();
      assertNotNull(status);
      assertTrue(status.getIsServer());
      assertTrue(status.getConnectedPeers().size()==1);
    }
    
    /**
     * 1. Initalize DS
     * 2. Initalize Caches
     * 3. Test the GemFireMemberStatus for the caches
     * 
     * @throws Exception if there's any exception while calling Admin APIs 
     */
    public void testInit() throws Exception {
      String testName = this.getName();
      initSystem(testName);
      initCache(testName);
      checkGFMemberStatus();
    }

    /**
     * Initalizes the VMs in the Admin DS. 
     * Cycles thro' all the hosts & VMs in each host and initilizes the VMs.
     * 
     * @param testName name of this test
     */
    protected void initSystem(final String testName) {
      SerializableRunnable runnable = new SerializableRunnable() {
        public void run() {
          Properties props  = getDistributedSystemProperties();
          Object     object = props.get(DistributionConfig.NAME_NAME);
          if (object == null) {
            //props.put(DistributionConfig.NAME_NAME, testName);
            getSystem(props);
          } else {
            getSystem();
          }
        }
      };
      
      int hostCount = Host.getHostCount();
      for (int i = 0; i < hostCount; i++) {
        Host host = Host.getHost(i);
        
        int noOfVMs = host.getVMCount();
        int j = 0;
        for (; j < noOfVMs; j++) {
          VM vm = host.getVM(j);
          
          vm.invoke(runnable);
          
          initedVMs.add(vm);
        }
        getLogWriter().info(j +" VMs initalized for host# "+i);
      }
    }
    
    /**
     * Initalizes cache. 
     * Checks whether the caches were created already.
     * If already not created, creates caches and checks valid them being not 
     * null.
     * 
     * @param testName name of this test
     * @throws AdminException if test fails while calling any admin API
     */
    private void initCache(String testName) throws AdminException {
      AdminDistributedSystem ads = this.tcSystem;
      
      SystemMember[] applnMembers = ads.getSystemMemberApplications();

      for (int i = 0; i < applnMembers.length; i++) {
        SystemMember      member = applnMembers[i];
        SystemMemberCache cache  = member.getCache();
        
        assertNull("Cache already created for member:"+member.getId(), cache);
      }
      
      for (Iterator iter = initedVMs.iterator(); iter.hasNext();) {
        VM vm = (VM) iter.next();
        vm.invoke(new CacheSerializableRunnable(testName) {
          public void run2() {
            CacheFactory.create(getSystem());
          }
        });
      }
      
      for (int i = 0; i < applnMembers.length; i++) {
        SystemMember      member = applnMembers[i];
        SystemMemberCache cache  = member.getCache();
        
        assertNotNull("Cache not created yet for member: "+member.getId(), cache);
      }
    }
    
    /**
     * Various tests for GemfireMemberStatus.
     * 1. Start bridge server randomly in any of the members.
     * 2. Checks the active bridge server count - 
     *    a. Checks GFMemberStatus & its string representation are initalized 
     *       properly.
     *    b. For a Bridge Server, checks whehter the connected peers are sme in 
     *       number as remaining no. of members.
     * 3. Check active bridge server count against expected
     * 4. Stops the BridgeServer.
     * 5. Repeat step 2 & 3.
     * 
     * @throws AdminException if test fails while calling any admin API
     * @throws InterruptedException If Thread is interupted while sleeping
     */
    private void checkGFMemberStatus() throws AdminException, InterruptedException {
      int serverIndex = startBridgeServer();
      
      int serverCount = checkActiveBridgeServerCount();
      assertEquals("No. of actual servers are not same as expected.", 1, serverCount);
      
      stopBridgeServer(serverIndex);
      pause(6 * 1000);
 
      serverCount = checkActiveBridgeServerCount();
      /*
       * TODO: After stoping the BridgeServer the same member shouldn't appear 
       * as a server. But it's still found as a server. Needed to be discussed.
       */      
      assertEquals("No. of actual servers are not same as expected.", 1, serverCount);      
    }
    
    private int startBridgeServer() throws AdminException, InterruptedException {
      AdminDistributedSystem ads = this.tcSystem;
      
      SystemMember[] applnMembers = ads.getSystemMemberApplications();
      
      int cacheIndex  = (int) (Math.random()*applnMembers.length);
      for (int i = 0; i < applnMembers.length; i++) {
        SystemMember      member = applnMembers[i];
        SystemMemberCache cache  = member.getCache();
        
        if (cacheIndex == i) {
          cache.addBridgeServer();
          pause(5 * 1000);
        }
      }
      
      return cacheIndex;
    }
    
    private int checkActiveBridgeServerCount() throws AdminException {
      AdminDistributedSystem ads = this.tcSystem;
      
      SystemMember[] applnMembers = ads.getSystemMemberApplications();
      int serverCount = 0;

      for (int i = 0; i < applnMembers.length; i++) {
        SystemMemberCache cache = applnMembers[i].getCache();
        
        GemFireMemberStatus status = ((SystemMemberCacheImpl)cache).getSnapshot();
        
        String snapShotString = status.toString();
        
        assertNotNull("String representation for GemFireMemberStatus is null.", 
            snapShotString);
        assertNotSame("Inappropriate string representation for GemFireMemberStatus", 
            snapShotString, "");
        
        if (status.getIsServer() && status.getIsConnected()) {
          Set connectedPeers = status.getConnectedPeers();
          
          assertEquals("Connected Peers for the server: "+applnMembers[i].getName()
              +" are inappropriate.", applnMembers.length-1, connectedPeers.size());
          serverCount++;
        }
      }
      
      return serverCount;
    }
    
    private void stopBridgeServer(int cacheIndex) 
      throws AdminException, InterruptedException {
      AdminDistributedSystem ads = this.tcSystem;
      
      SystemMember[] applnMembers = ads.getSystemMemberApplications();
      
      SystemMemberBridgeServer[] bridgeServers = 
        applnMembers[cacheIndex].getCache().getBridgeServers();
      
      getLogWriter().info("No of Bridge Servers:: "+bridgeServers.length);
      
      for (int i = 0; i < bridgeServers.length; i++) {
        SystemMemberBridgeServer server = bridgeServers[i];
        server.stop();
        
        server.refresh();
      }
      applnMembers[cacheIndex].getCache().refresh();
      pause(5 * 1000);
    }
}
