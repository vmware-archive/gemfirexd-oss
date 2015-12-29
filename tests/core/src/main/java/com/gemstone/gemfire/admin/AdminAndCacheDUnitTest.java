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

import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.admin.internal.DistributionLocatorImpl;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.internal.SocketCreator;

import java.util.*;
import dunit.*;

/**
 * Tests the interaction of the {@link com.gemstone.gemfire.cache
 * Cache} and {@link com.gemstone.gemfire.admin admin} APIs in the
 * same VM.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class AdminAndCacheDUnitTest extends CacheTestCase {

  /**
   * Creates a new <code>AdminAndCacheDUnitTest</code> with the given
   * name.
   */
  public AdminAndCacheDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Makes sure that the admin API can be used to see a VM's own
   * cache and regions.
   */
  public void testCacheAndRegions() throws Exception {
    disconnectAllFromDS();

    InternalDistributedSystem system = this.getSystem();
    assertTrue(!system.getConfig().getLocators().equals(""));

    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    assertEquals(system.getConfig().getLocators(),
                 config.getLocators());
    assertEquals(system.getConfig().getMcastPort(),
                 config.getMcastPort());

    getLogWriter().info("Locators are: " + config.getLocators() +
                        ", mcast port is: " + config.getMcastPort());


    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    assertTrue(admin.waitToBeConnected(5 * 1000));

    assertEquals(1, admin.getSystemMemberApplications().length);

    SystemMember me =
      admin.getSystemMemberApplications()[0];
    assertNotNull(me);
    assertFalse(me.hasCache());

    this.getCache();
    assertTrue(me.hasCache());
    
    SystemMemberCache adminCache = me.getCache();
    assertNotNull(adminCache);
    assertEquals(0, adminCache.getRootRegionNames().size());

    RegionAttributes attrs =
      (new AttributesFactory()).create();
    String rootName = "root";
    createRootRegion(rootName, attrs);
    
    adminCache.refresh();
    assertEquals(1, adminCache.getRootRegionNames().size());
    assertEquals(rootName,
                 adminCache.getRootRegionNames().iterator().next());
  }

  /**
   * Tests that the admin API launched in an application VM can see
   * caches and regions in other members of the distributed system.
   */
  public void testOtherCacheAndRegions() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    disconnectAllFromDS();
    assertNull(AdminDistributedSystemImpl.getConnectedInstance());
    
    ExpirationAttributes ttl =
      new ExpirationAttributes(300, ExpirationAction.DESTROY);
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setEntryIdleTimeout(ttl);
    final RegionAttributes attrs = factory.create();
    final String rootName = this.getUniqueName();

    Runnable connect = new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          createRootRegion(rootName, attrs);
        }
      };

    vm0.invoke(connect);
    vm1.invoke(connect);
    connect.run();

    DistributedSystem system = this.getSystem();
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    assertTrue(admin.waitToBeConnected(5 * 1000));

    SystemMember[] members = admin.getSystemMemberApplications();
    if (members.length != 3) {
      StringBuffer sb = new StringBuffer();
      sb.append(members.length);
      sb.append(" members: ");
      for (int i = 0; i < members.length; i++) {
        sb.append(members[i]);
        sb.append(" ");
      }
      assertEquals(sb.toString(), 3, members.length);
    }

    for (int i = 0; i < members.length; i++) {
      SystemMember member = members[i];
      assertTrue(member.hasCache());
      SystemMemberCache cache = member.getCache();
      Set roots = cache.getRootRegionNames();
      assertEquals(1, roots.size());
      assertEquals(rootName, roots.iterator().next());

      SystemMemberRegion root = cache.getRegion(rootName);
      assertNotNull(root);
      assertEquals(0, root.getEntryCount());
      assertEquals(ttl.getTimeout(),
                   root.getEntryIdleTimeoutTimeLimit());
      assertEquals(ttl.getAction(), root.getEntryIdleTimeoutAction());
    }
  }

  /**
   * Test that disconnecting a colocated admin client from a normal peer
   * removes the admin client from the normal member list on the peer.
   * The bug is caused by the client disconnecting the admin first followed by
   * the normal ds disconnect.
   */
  public void testBug39747() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    disconnectAllFromDS();
    assertNull(AdminDistributedSystemImpl.getConnectedInstance());
    
    ExpirationAttributes ttl =
      new ExpirationAttributes(300, ExpirationAction.DESTROY);
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setEntryIdleTimeout(ttl);
    final RegionAttributes attrs = factory.create();
    final String rootName = this.getUniqueName();

    Runnable connect = new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          createRootRegion(rootName, attrs);
        }
      };

    vm0.invoke(connect);
    connect.run();

    DistributedSystem system = getSystem();
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    assertTrue(admin.waitToBeConnected(5 * 1000));
    Runnable verify0 = new SerializableRunnable("verify0") {
        public void run() {
          InternalDistributedSystem ds = getSystem();
          int retry = 100;
          while (retry-- > 0 && ds.getDistributionManager().getAdminMemberSet().size() != 1) {
            pause(100);
          }
          assertEquals(1, ds.getDistributionManager().getAdminMemberSet().size());
          assertEquals(1, ds.getDistributionManager().getOtherNormalDistributionManagerIds().size());
        }
      };
    vm0.invoke(verify0);

    admin.disconnect();
    // verify that he is no longer an admin member but still a normal member
    Runnable verify1 = new SerializableRunnable("verify1") {
        public void run() {
          InternalDistributedSystem ds = getSystem();
          int retry = 100;
          while (retry-- > 0 && ds.getDistributionManager().getAdminMemberSet().size() > 0) {
            pause(100);
          }
          assertEquals(new HashSet(), ds.getDistributionManager().getAdminMemberSet());
          assertEquals(1, ds.getDistributionManager().getOtherNormalDistributionManagerIds().size());
        }
      };
    vm0.invoke(verify1);

    system.disconnect();

    // now verify that he is no longer a member of the dm
    Runnable verify2 = new SerializableRunnable("verify2") {
        public void run() {
          InternalDistributedSystem ds = getSystem();
          int retry = 100;
          while (retry-- > 0 && ds.getDistributionManager().getOtherNormalDistributionManagerIds().size() > 0) {
            pause(100);
          }
          assertEquals(new HashSet(), ds.getDistributionManager().getOtherNormalDistributionManagerIds());
        }
      };
    vm0.invoke(verify2);
  }

  /**
   * Make sure that admin stuff gets cleaned up on peer DMs when the DistributedSystem
   * shutsdown but the adminDS does not.
   */
  public void testNoAdminDisconnect() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    disconnectAllFromDS();
    assertNull(AdminDistributedSystemImpl.getConnectedInstance());
    
    ExpirationAttributes ttl =
      new ExpirationAttributes(300, ExpirationAction.DESTROY);
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setEntryIdleTimeout(ttl);
    final RegionAttributes attrs = factory.create();
    final String rootName = this.getUniqueName();

    Runnable connect = new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          createRootRegion(rootName, attrs);
        }
      };

    vm0.invoke(connect);
    connect.run();

    DistributedSystem system = getSystem();
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    assertTrue(admin.waitToBeConnected(5 * 1000));
    Runnable verify0 = new SerializableRunnable("verify0") {
        public void run() {
          InternalDistributedSystem ds = getSystem();
          int retry = 100;
          while (retry-- > 0 && ds.getDistributionManager().getAdminMemberSet().size() != 1) {
            pause(100);
          }
          assertEquals(1, ds.getDistributionManager().getAdminMemberSet().size());
          assertEquals(1, ds.getDistributionManager().getOtherNormalDistributionManagerIds().size());
        }
      };
    vm0.invoke(verify0);

    system.disconnect();

    // now verify that he is no longer a member of the dm
    Runnable verify2 = new SerializableRunnable("verify2") {
        public void run() {
          InternalDistributedSystem ds = getSystem();
          int retry = 100;
          while (retry-- > 0 && ds.getDistributionManager().getAdminMemberSet().size() > 0) {
            pause(100);
          }
          assertEquals(new HashSet(), ds.getDistributionManager().getOtherNormalDistributionManagerIds());
          assertEquals(0, ds.getDistributionManager().getAdminMemberSet().size());
        }
      };
    vm0.invoke(verify2);
  }
  
  public void testDistributionLocatorIsRunning() throws Exception {
    disconnectAllFromDS();

    Properties props = new Properties();
    props.setProperty("locators", SocketCreator.getLocalHost().getHostAddress()
        + '[' + getDUnitLocatorPort() + ']');
    InternalDistributedSystem system = this.getSystem(props);
    assertTrue(!system.getConfig().getLocators().equals(""));

    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    assertEquals(system.getConfig().getLocators(),
                 config.getLocators());
    assertEquals(system.getConfig().getMcastPort(),
                 config.getMcastPort());

    config.setRemoteCommand("");
    
    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    assertTrue(admin.waitToBeConnected(5 * 1000));

    assertEquals(1, admin.getDistributionLocators().length);
    
    DistributionLocator[] locators = admin.getDistributionLocators();
    final DistributionLocatorImpl locator = (DistributionLocatorImpl)locators[0];
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return locator.isRunning();
      }
      public String description() {
        return "locator.isRunning()";
      }
    };
    waitForCriterion(wc, 5000, 10, true);
  }
}
