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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.SystemMembershipEvent;
import com.gemstone.gemfire.admin.SystemMembershipListener;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.util.BridgeMembership;
import com.gemstone.gemfire.cache.util.BridgeMembershipEvent;
import com.gemstone.gemfire.cache.util.BridgeMembershipListener;
import com.gemstone.gemfire.cache.util.UniversalMembershipListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DurableClientAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.tier.InternalBridgeMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests the UniversalMembershipListenerAdapter.
 *
 * @author Kirk Lund
 * @since 4.2.1
 */
public class UniversalMembershipListenerAdapterDUnitTest extends BridgeTestCase {

  protected static final boolean CLIENT = true;
  protected static final boolean SERVER = false;
  
  protected static final int JOINED = 0;
  protected static final int LEFT = 1;
  protected static final int CRASHED = 2;
  
  /** Brief pause for basic testing of asynchronous event notification */
  private static final int BRIEF_PAUSE_MILLIS = 2000;
  /** Maximum millis allowed for bridge client to fully connect before test fails */
  private static final int JOIN_FAIL_MILLIS = 120000;
  
  // the following wait millis are max wait time until notify occurs 
  
  /** Millis to wait for connection to AdminDS */
  private static final int CONNECT_WAIT_MILLIS = 5000;
  /** Millis to wait for basic synchronous listener to be notified */
  private static final int SYNC_ASYNC_EVENT_WAIT_MILLIS = 2000;
  /** Millis to wait for all three event listeners to be notified */
  private static final int ASYNC_EVENT_WAIT_MILLIS = 120000; // use Integer.MAX_VALUE for debugging
    
  public UniversalMembershipListenerAdapterDUnitTest(String name) {
    super(name);
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    InternalBridgeMembership.unregisterAllListeners();
  }
  
  /**
   * Tests wrapping of BridgeMembershipEvent fired as SystemMembershipEvent.
   */
  public void testAdaptedBridgeEvents() throws Exception {
    getSystem();
    
    final boolean[] fired = new boolean[1];
    final DistributedMember[] member = new DistributedMember[1];
    final String[] memberId = new String[1];
    
    UniversalMembershipListenerAdapter listener = 
    new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(SystemMembershipEvent event) {
        assertFalse(fired[0]); // assures no dupes
        assertNull(member[0]);
        assertNull(memberId[0]);
        fired[0] = true;
        member[0] = event.getDistributedMember();
        memberId[0] = event.getMemberId();
        notify();
      }
      @Override
      public void memberLeft(SystemMembershipEvent event) {
      }
      @Override
      public void memberCrashed(SystemMembershipEvent event) {
      }
    };
    
    DistributedMember clientJoined = new TestDistributedMember("clientJoined");
    InternalBridgeMembership.notifyJoined(clientJoined, true);
    synchronized(listener) {
      if (!fired[0]) {
        listener.wait(SYNC_ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    assertTrue(fired[0]);
    assertEquals(clientJoined, member[0]);
    assertEquals(clientJoined.getId(), memberId[0]);
  }
  
  /**
   * Tests use of history to prevent duplicate events.
   */
  public void testNoDuplicates() throws Exception {
    getSystem();
    
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    
    UniversalMembershipListenerAdapter listener = 
    new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(SystemMembershipEvent event) {
        assertFalse(fired[JOINED]);
        assertNull(member[JOINED]);
        assertNull(memberId[JOINED]);
        fired[JOINED] = true;
        member[JOINED] = event.getDistributedMember();
        memberId[JOINED] = event.getMemberId();
        notify();
      }
      @Override
      public synchronized void memberLeft(SystemMembershipEvent event) {
        assertFalse(fired[LEFT]);
        assertNull(member[LEFT]);
        assertNull(memberId[LEFT]);
        fired[LEFT] = true;
        member[LEFT] = event.getDistributedMember();
        memberId[LEFT] = event.getMemberId();
        notify();
      }
      @Override
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        assertFalse(fired[CRASHED]); // assures no dupes
        assertNull(member[CRASHED]);
        assertNull(memberId[CRASHED]);
        fired[CRASHED] = true;
        member[CRASHED] = event.getDistributedMember();
        memberId[CRASHED] = event.getMemberId();
        notify();
      }
    };
    
    DistributedMember memberA = new TestDistributedMember("memberA");
    
    // first join
    InternalBridgeMembership.notifyJoined(memberA, true);
    synchronized(listener) {
      if (!fired[JOINED]) {
        listener.wait(SYNC_ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    assertTrue(fired[JOINED]);
    assertEquals(memberA, member[JOINED]);
    assertEquals(memberA.getId(), memberId[JOINED]);
    fired[JOINED] = false;
    member[JOINED] = null;
    memberId[JOINED] = null;

    // duplicate join
    InternalBridgeMembership.notifyJoined(memberA, true);
    pause(BRIEF_PAUSE_MILLIS);
    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);

    // first left
    InternalBridgeMembership.notifyLeft(memberA, true);
    synchronized(listener) {
      if (!fired[LEFT]) {
        listener.wait(SYNC_ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    assertTrue(fired[LEFT]);
    assertEquals(memberA, member[LEFT]);
    assertEquals(memberA.getId(), memberId[LEFT]);
    fired[LEFT] = false;
    member[LEFT] = null;
    memberId[LEFT] = null;

    // duplicate left
    InternalBridgeMembership.notifyLeft(memberA, true);
    pause(BRIEF_PAUSE_MILLIS);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    
    // rejoin
    InternalBridgeMembership.notifyJoined(memberA, true);
    synchronized(listener) {
      if (!fired[JOINED]) {
        listener.wait(SYNC_ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    assertTrue(fired[JOINED]);
    assertEquals(memberA, member[JOINED]);
    assertEquals(memberA.getId(), memberId[JOINED]);
  }
  
  /**
   * Tests notification of events for loner bridge clients in server process.
   */
  public void testLonerClientEventsInServer() throws Exception {
     try {
       doTestLonerClientEventsInServer();
     }
     finally {
       disconnectAllFromDS();
     }
  }
  private void doTestLonerClientEventsInServer() throws Exception {
    final boolean[] firedSystem = new boolean[3];
    final DistributedMember[] memberSystem = new DistributedMember[3];
    final String[] memberIdSystem = new String[3];
    final boolean[] isClientSystem = new boolean[3];

    final boolean[] firedAdapter = new boolean[3];
    final DistributedMember[] memberAdapter = new DistributedMember[3];
    final String[] memberIdAdapter = new String[3];
    final boolean[] isClientAdapter = new boolean[3];

    final boolean[] firedBridge = new boolean[3];
    final DistributedMember[] memberBridge = new DistributedMember[3];
    final String[] memberIdBridge = new String[3];
    final boolean[] isClientBridge = new boolean[3];

    SystemMembershipListener systemListener = new SystemMembershipListener() {
      public synchronized void memberJoined(SystemMembershipEvent event) {
        assertFalse(firedSystem[JOINED]);
        assertNull(memberSystem[JOINED]);
        assertNull(memberIdSystem[JOINED]);
        firedSystem[JOINED] = true;
        memberSystem[JOINED] = event.getDistributedMember();
        memberIdSystem[JOINED] = event.getMemberId();
        notify();
      }
      public synchronized void memberLeft(SystemMembershipEvent event) {
        assertFalse(firedSystem[LEFT]);
        assertNull(memberSystem[LEFT]);
        assertNull(memberIdSystem[LEFT]);
        firedSystem[LEFT] = true;
        memberSystem[LEFT] = event.getDistributedMember();
        memberIdSystem[LEFT] = event.getMemberId();
        notify();
      }
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        assertFalse(firedSystem[CRASHED]);
        assertNull(memberSystem[CRASHED]);
        assertNull(memberIdSystem[CRASHED]);
        firedSystem[CRASHED] = true;
        memberSystem[CRASHED] = event.getDistributedMember();
        memberIdSystem[CRASHED] = event.getMemberId();
        notify();
      }
    };

    UniversalMembershipListenerAdapter adapter = 
      new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(SystemMembershipEvent event) {
        assertFalse(firedAdapter[JOINED]);
        assertNull(memberAdapter[JOINED]);
        assertNull(memberIdAdapter[JOINED]);
        assertFalse(isClientAdapter[JOINED]);
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        isClientAdapter[JOINED] = ((UniversalMembershipListenerAdapter.
          AdaptedMembershipEvent)event).isClient();
        notify();
      }
      @Override
      public synchronized void memberLeft(SystemMembershipEvent event) {
        assertFalse(firedAdapter[LEFT]);
        assertNull(memberAdapter[LEFT]);
        assertNull(memberIdAdapter[LEFT]);
        assertFalse(isClientAdapter[LEFT]);
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        isClientAdapter[LEFT] = ((UniversalMembershipListenerAdapter.
          AdaptedMembershipEvent)event).isClient();
        notify();
      }
      @Override
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        assertFalse(firedAdapter[CRASHED]);
        assertNull(memberAdapter[CRASHED]);
        assertNull(memberIdAdapter[CRASHED]);
        assertFalse(isClientAdapter[CRASHED]);
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        isClientAdapter[CRASHED] = ((UniversalMembershipListenerAdapter.
          AdaptedMembershipEvent)event).isClient();
        notify();
      }
    };

    BridgeMembershipListener bridgeListener = new BridgeMembershipListener() {
      public synchronized void memberJoined(BridgeMembershipEvent event) {
        assertFalse(firedBridge[JOINED]);
        assertNull(memberBridge[JOINED]);
        assertNull(memberIdBridge[JOINED]);
        assertFalse(isClientBridge[JOINED]);
        firedBridge[JOINED] = true;
        memberBridge[JOINED] = event.getMember();
        memberIdBridge[JOINED] = event.getMemberId();
        isClientBridge[JOINED] = event.isClient();
        notify();
      }
      public synchronized void memberLeft(BridgeMembershipEvent event) {
        assertFalse(firedBridge[LEFT]);
        assertNull(memberBridge[LEFT]);
        assertNull(memberIdBridge[LEFT]);
        assertFalse(isClientBridge[LEFT]);
        firedBridge[LEFT] = true;
        memberBridge[LEFT] = event.getMember();
        memberIdBridge[LEFT] = event.getMemberId();
        isClientBridge[LEFT] = event.isClient();
        notify();
      }
      public synchronized void memberCrashed(BridgeMembershipEvent event) {
        assertFalse(firedBridge[CRASHED]);
        assertNull(memberBridge[CRASHED]);
        assertNull(memberIdBridge[CRASHED]);
        assertFalse(isClientBridge[CRASHED]);
        firedBridge[CRASHED] = true;
        memberBridge[CRASHED] = event.getMember();
        memberIdBridge[CRASHED] = event.getMemberId();
        isClientBridge[CRASHED] = event.isClient();
        notify();
      }
    };
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    // create BridgeServer in controller vm...
    getLogWriter().info("[testLonerClientEventsInServer] Create BridgeServer");
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertNotNull(region);
    assertNotNull(getRootRegion().getSubregion(name));
    
    ports[0] = startBridgeServer(0);
    assertTrue(ports[0] != 0);
    final String serverMemberId = getMemberId();
    final DistributedMember serverMember = getDistributedMember();
    final Properties serverProperties = getSystem().getProperties();

    getLogWriter().info("[testLonerClientEventsInServer] ports[0]=" + ports[0]);
    getLogWriter().info("[testLonerClientEventsInServer] serverMemberId=" + serverMemberId);
    getLogWriter().info("[testLonerClientEventsInServer] serverMember=" + serverMember);

    // register the bridge listener
    BridgeMembership.registerBridgeMembershipListener(bridgeListener);
    
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), null);
    AdminDistributedSystem adminDS = 
      AdminDistributedSystemFactory.getDistributedSystem(config);
    adminDS.connect();
    assertTrue(adminDS.waitToBeConnected(CONNECT_WAIT_MILLIS));

    // register the system listener
    adminDS.addMembershipListener(systemListener);

    // register the universal adapter
    adapter.registerMembershipListener(adminDS);

    SerializableRunnable createBridgeClient =
    new CacheSerializableRunnable("Create bridge client") {
      @Override
      public void run2() throws CacheException {
        getLogWriter().info("[testLonerClientEventsInServer] create bridge client");
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
      }
    };
    SerializableRunnable createSystemBridgeClient = // TODO bug36296
    new CacheSerializableRunnable("Create system bridge client") {
      @Override
      public void run2() throws CacheException {
        getLogWriter().info("[testLonerClientEventsInServer] create system bridge client");
        getSystem(serverProperties);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
      }
    };

    // create bridge client in vm0...
    vm0.invoke(createBridgeClient);
    String clientMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    DistributedMember clientMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");
                                                
    // should trigger both adapter and bridge listener but not system listener
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testLonerClientEventsInServer] assert server detected client join");
    assertTrue(firedBridge[JOINED]);
    assertEquals(clientMember, memberBridge[JOINED]);
    //as of 6.1 the id can change when a bridge is created or a connection pool is created
    //assertEquals(clientMemberId, memberIdBridge[JOINED]);
    assertTrue(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertEquals(clientMember, memberAdapter[JOINED]);
    assertEquals(clientMemberId, memberIdAdapter[JOINED]);
    assertTrue(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      public void run() {
        getLogWriter().info("[testLonerClientEventsInServer] wait for client to fully connect");
        final String pl =
          getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pi = (PoolImpl)PoolManager.find(pl);
        waitForClientToFullyConnect(pi);
      }
    });
    
    vm0.invoke(new SerializableRunnable("Close bridge client region") {
      public void run() {
        getLogWriter().info("[testLonerClientEventsInServer] close bridge client region");
        getRootRegion().getSubregion(name).close();
        PoolManager.close();
      }
    });

    synchronized(adapter) {
      if (!firedAdapter[LEFT]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[LEFT]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testLonerClientEventsInServer] assert server detected client left");

    assertFalse(firedBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertTrue(firedBridge[LEFT]);
    assertEquals(clientMember, memberBridge[LEFT]);
    assertEquals(clientMemberId, memberIdBridge[LEFT]);
    assertTrue(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertFalse(firedAdapter[JOINED]);
    assertNull(memberAdapter[JOINED]);
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertTrue(firedAdapter[LEFT]);
    assertEquals(clientMember, memberAdapter[LEFT]);
    assertEquals(clientMemberId, memberIdAdapter[LEFT]);
    assertTrue(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    // reconnect bridge client to test for crashed event
    vm0.invoke(createBridgeClient);
    clientMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    clientMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");
                                                
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testLonerClientEventsInServer] assert server detected client re-join");
    assertTrue(firedBridge[JOINED]);
    assertEquals(clientMember, memberBridge[JOINED]);
    assertEquals(clientMemberId, memberIdBridge[JOINED]);
    assertTrue(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertEquals(clientMember, memberAdapter[JOINED]);
    assertEquals(clientMemberId, memberIdAdapter[JOINED]);
    assertTrue(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      public void run() {
        getLogWriter().info("[testLonerClientEventsInServer] wait for client to fully connect");
        final String pl =
          getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pi = (PoolImpl)PoolManager.find(pl);
        waitForClientToFullyConnect(pi);
      }
    });

    ServerConnection.setForceClientCrashEvent(true);
    try {
      vm0.invoke(new SerializableRunnable("Stop bridge client") {
        public void run() {
          getLogWriter().info("[testLonerClientEventsInServer] Stop bridge client");
          getRootRegion().getSubregion(name).close();
          PoolManager.close();
        }
      });
  
      synchronized(adapter) {
        if (!firedAdapter[CRASHED]) {
          adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
      synchronized(bridgeListener) {
        if (!firedBridge[CRASHED]) {
          bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
      
    getLogWriter().info("[testLonerClientEventsInServer] assert server detected client crashed");
    assertFalse(firedBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertTrue(firedBridge[CRASHED]);
    assertEquals(clientMember, memberBridge[CRASHED]);
    assertEquals(clientMemberId, memberIdBridge[CRASHED]);
    assertTrue(isClientBridge[CRASHED]);
//    resetArraysForTesting(firedBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
//    resetArraysForTesting(firedSystem, memberIdSystem, isClientSystem);

    assertFalse(firedAdapter[JOINED]);
    assertNull(memberAdapter[JOINED]);
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertTrue(firedAdapter[CRASHED]);
    assertEquals(clientMember, memberAdapter[CRASHED]);
    assertEquals(clientMemberId, memberIdAdapter[CRASHED]);
    assertTrue(isClientAdapter[CRASHED]);
//    resetArraysForTesting(firedAdapter, memberIdAdapter, isClientAdapter);
    }
    finally {
      ServerConnection.setForceClientCrashEvent(false);
    }
  }
  
  /**
   * Tests notification of events for loner bridge clients in server process.
   */
  public void testSystemClientEventsInServer() throws Exception {
     try {
       doTestSystemClientEventsInServer();
     }
     finally {
       disconnectAllFromDS();
     }
  }
  private void doTestSystemClientEventsInServer() throws Exception {
    final boolean[] firedSystem = new boolean[3];
    final DistributedMember[] memberSystem = new DistributedMember[3];
    final String[] memberIdSystem = new String[3];
    final boolean[] isClientSystem = new boolean[3];

    final boolean[] firedAdapter = new boolean[3];
    final DistributedMember[] memberAdapter = new DistributedMember[3];
    final String[] memberIdAdapter = new String[3];
    final boolean[] isClientAdapter = new boolean[3];

    final boolean[] firedBridge = new boolean[3];
    final DistributedMember[] memberBridge = new DistributedMember[3];
    final String[] memberIdBridge = new String[3];
    final boolean[] isClientBridge = new boolean[3];
    
    final boolean[] firedSystemDuplicate = new boolean[3];
    final boolean[] firedAdapterDuplicate = new boolean[3];
    final boolean[] firedBridgeDuplicate = new boolean[3];
    
    SystemMembershipListener systemListener = new SystemMembershipListener() {
      public synchronized void memberJoined(SystemMembershipEvent event) {
        firedSystemDuplicate[JOINED] = firedSystem[JOINED];
        firedSystem[JOINED] = true;
        memberSystem[JOINED] = event.getDistributedMember();
        memberIdSystem[JOINED] = event.getMemberId();
        notify();
      }
      public synchronized void memberLeft(SystemMembershipEvent event) {
        firedSystemDuplicate[LEFT] = firedSystem[LEFT];
        firedSystem[LEFT] = true;
        memberSystem[LEFT] = event.getDistributedMember();
        memberIdSystem[LEFT] = event.getMemberId();
        notify();
      }
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        firedSystemDuplicate[CRASHED] = firedSystem[CRASHED];
        firedSystem[CRASHED] = true;
        memberSystem[CRASHED] = event.getDistributedMember();
        memberIdSystem[CRASHED] = event.getMemberId();
        notify();
      }
    };

    UniversalMembershipListenerAdapter adapter = 
      new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(SystemMembershipEvent event) {
        getLogWriter().info("[doTestSystemClientEventsInServer] memberJoined >" + event.getMemberId() + "<");
        firedAdapterDuplicate[JOINED] = firedAdapter[JOINED];
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[JOINED] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
      @Override
      public synchronized void memberLeft(SystemMembershipEvent event) {
        getLogWriter().info("[doTestSystemClientEventsInServer] memberLeft >" + event.getMemberId() + "<");
        firedAdapterDuplicate[LEFT] = firedAdapter[LEFT];
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[LEFT] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
      @Override
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        getLogWriter().info("[doTestSystemClientEventsInServer] memberCrashed >" + event.getMemberId() + "<");
        firedAdapterDuplicate[CRASHED] = firedAdapter[CRASHED];
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[CRASHED] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
    };

    BridgeMembershipListener bridgeListener = new BridgeMembershipListener() {
      public synchronized void memberJoined(BridgeMembershipEvent event) {
        firedBridgeDuplicate[JOINED] = firedBridge[JOINED];
        firedBridge[JOINED] = true;
        memberBridge[JOINED] = event.getMember();
        memberIdBridge[JOINED] = event.getMemberId();
        isClientBridge[JOINED] = event.isClient();
        notify();
      }
      public synchronized void memberLeft(BridgeMembershipEvent event) {
        firedBridgeDuplicate[LEFT] = firedBridge[LEFT];
        firedBridge[LEFT] = true;
        memberBridge[LEFT] = event.getMember();
        memberIdBridge[LEFT] = event.getMemberId();
        isClientBridge[LEFT] = event.isClient();
        notify();
      }
      public synchronized void memberCrashed(BridgeMembershipEvent event) {
        firedBridgeDuplicate[CRASHED] = firedBridge[CRASHED];
        firedBridge[CRASHED] = true;
        memberBridge[CRASHED] = event.getMember();
        memberIdBridge[CRASHED] = event.getMemberId();
        isClientBridge[CRASHED] = event.isClient();
        notify();
      }
    };
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    // create BridgeServer in controller vm...
    getLogWriter().info("[doTestSystemClientEventsInServer] Create BridgeServer");
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertNotNull(region);
    assertNotNull(getRootRegion().getSubregion(name));
    
    ports[0] = startBridgeServer(0);
    assertTrue(ports[0] != 0);
    final String serverMemberId = getMemberId();
    final DistributedMember serverMember = getDistributedMember();
    final Properties serverProperties = getSystem().getProperties();

    getLogWriter().info("[doTestSystemClientEventsInServer] ports[0]=" + ports[0]);
    getLogWriter().info("[doTestSystemClientEventsInServer] serverMemberId=" + serverMemberId);
    getLogWriter().info("[doTestSystemClientEventsInServer] serverMember=" + serverMember);

    // register the bridge listener
    BridgeMembership.registerBridgeMembershipListener(bridgeListener);
    
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), null);
    AdminDistributedSystem adminDS = 
      AdminDistributedSystemFactory.getDistributedSystem(config);
    adminDS.connect();
    assertTrue(adminDS.waitToBeConnected(CONNECT_WAIT_MILLIS));

    // register the system listener
    adminDS.addMembershipListener(systemListener);

    // register the universal adapter
    adapter.registerMembershipListener(adminDS);

    SerializableRunnable createBridgeClient =
    new CacheSerializableRunnable("Create bridge client") {
      @Override
      public void run2() throws CacheException {
        getLogWriter().info("[doTestSystemClientEventsInServer] create system bridge client");
        assertTrue(getSystem(serverProperties).isConnected());
        assertFalse(getCache().isClosed());
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
      }
    };

    // create bridge client in vm0...
    vm0.invoke(createBridgeClient);
    String clientMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    DistributedMember clientMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");
                                                
    // should trigger both adapter and bridge listener but not system listener
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(systemListener) {
      if (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[doTestSystemClientEventsInServer] assert server detected client join");
    assertFalse(firedSystemDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertEquals(clientMember, memberBridge[JOINED]);
    assertEquals(clientMemberId, memberIdBridge[JOINED]);
    assertTrue(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertTrue(firedSystem[JOINED]);
    assertEquals(clientMember, memberSystem[JOINED]);
    assertEquals(clientMemberId, memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertEquals(clientMember, memberAdapter[JOINED]);
    assertEquals(clientMemberId, memberIdAdapter[JOINED]);
    //assertTrue(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      public void run() {
        getLogWriter().info("[doTestSystemClientEventsInServer] wait for client to fully connect");
        final String pl =
          getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pi = (PoolImpl)PoolManager.find(pl);
        waitForClientToFullyConnect(pi);
      }
    });
    
    // close bridge client region
    vm0.invoke(new SerializableRunnable("Close bridge client region") {
      public void run() {
        getLogWriter().info("[doTestSystemClientEventsInServer] close bridge client region");
        getRootRegion().getSubregion(name).close();
        PoolManager.close();
      }
    });

    synchronized(adapter) {
      if (!firedAdapter[LEFT]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[LEFT]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[doTestSystemClientEventsInServer] assert server detected client left");
    assertFalse(firedSystemDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);
    
    assertFalse(firedBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertTrue(firedBridge[LEFT]);
    assertEquals(clientMember, memberBridge[LEFT]);
    assertEquals(clientMemberId, memberIdBridge[LEFT]);
    assertTrue(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertFalse(firedAdapter[JOINED]);
    assertNull(memberAdapter[JOINED]);
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertTrue(firedAdapter[LEFT]);
    assertEquals(clientMember, memberAdapter[LEFT]);
    assertEquals(clientMemberId, memberIdAdapter[LEFT]);
    assertTrue(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    // reconnect bridge client
    vm0.invoke(createBridgeClient);
    clientMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    clientMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");
                                                
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[doTestSystemClientEventsInServer] assert server detected client re-join");
    assertFalse(firedSystemDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertEquals(clientMember, memberBridge[JOINED]);
    assertEquals(clientMemberId, memberIdBridge[JOINED]);
    assertTrue(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertEquals(clientMember, memberAdapter[JOINED]);
    assertEquals(clientMemberId, memberIdAdapter[JOINED]);
    //assertTrue(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      public void run() {
        getLogWriter().info("[doTestSystemClientEventsInServer] wait for client to fully connect");
        final String pl =
          getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pi = (PoolImpl)PoolManager.find(pl);
        waitForClientToFullyConnect(pi);
      }
    });

    // have bridge client disconnect from system
    vm0.invoke(new SerializableRunnable("Disconnect bridge client") {
      public void run() {
        getLogWriter().info("[doTestSystemClientEventsInServer] disconnect bridge client");
        closeCache();
        disconnectFromDS();
      }
    });

    synchronized(adapter) {
      if (!firedAdapter[LEFT]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(systemListener) {
      if (!firedSystem[LEFT]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[LEFT]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[doTestSystemClientEventsInServer] assert server detected client left");
    assertFalse(firedSystemDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertFalse(firedBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertTrue(firedBridge[LEFT]);
    assertEquals(clientMember, memberBridge[LEFT]);
    assertEquals(clientMemberId, memberIdBridge[LEFT]);
    assertTrue(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertTrue(firedSystem[LEFT]);
    assertEquals(clientMember, memberSystem[LEFT]);
    assertEquals(clientMemberId, memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertFalse(firedAdapter[JOINED]);
    assertNull(memberAdapter[JOINED]);
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertTrue(firedAdapter[LEFT]);
    assertEquals(clientMember, memberAdapter[LEFT]);
    assertEquals(clientMemberId, memberIdAdapter[LEFT]);
    //assertTrue(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    // reconnect bridge client
    vm0.invoke(createBridgeClient);
    clientMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    clientMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");
                                                
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(systemListener) {
      if (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[doTestSystemClientEventsInServer] assert server detected client re-join");
    assertFalse(firedSystemDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertEquals(clientMember, memberBridge[JOINED]);
    assertEquals(clientMemberId, memberIdBridge[JOINED]);
    assertTrue(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertTrue(firedSystem[JOINED]);
    assertEquals(clientMember, memberSystem[JOINED]);
    assertEquals(clientMemberId, memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertEquals(clientMember, memberAdapter[JOINED]);
    assertEquals(clientMemberId, memberIdAdapter[JOINED]);
    //assertTrue(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      public void run() {
        getLogWriter().info("[doTestSystemClientEventsInServer] wait for client to fully connect");
        final String pl =
          getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pi = (PoolImpl)PoolManager.find(pl);
        waitForClientToFullyConnect(pi);
      }
    });

    // close bridge client region with test hook for crash
    ServerConnection.setForceClientCrashEvent(true);
    try {
      vm0.invoke(new SerializableRunnable("Close bridge client region") {
        public void run() {
          getLogWriter().info("[doTestSystemClientEventsInServer] close bridge client region");
          getRootRegion().getSubregion(name).close();
          PoolManager.close();
        }
      });
  
      synchronized(adapter) {
        if (!firedAdapter[CRASHED]) {
          adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
      synchronized(bridgeListener) {
        if (!firedBridge[CRASHED]) {
          bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
      
    getLogWriter().info("[doTestSystemClientEventsInServer] assert server detected client crashed");
    assertFalse(firedSystemDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertFalse(firedBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertTrue(firedBridge[CRASHED]);
    assertEquals(clientMember, memberBridge[CRASHED]);
    assertEquals(clientMemberId, memberIdBridge[CRASHED]);
    assertTrue(isClientBridge[CRASHED]);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);

    assertFalse(firedAdapter[JOINED]);
    assertNull(memberAdapter[JOINED]);
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertTrue(firedAdapter[CRASHED]);
    assertEquals(clientMember, memberAdapter[CRASHED]);
    assertEquals(clientMemberId, memberIdAdapter[CRASHED]);
    assertTrue(isClientAdapter[CRASHED]);
    }
    finally {
      ServerConnection.setForceClientCrashEvent(false);
    }
  }

  /**
   * Waits for client to create {@link 
   * com.gemstone.gemfire.internal.cache.tier.Endpoint#getNumConnections
   * Endpoint.getNumConnections()} to {@link 
   * com.gemstone.gemfire.internal.cache.tier.Endpoint}. Note: This probably
   * won't work if the BridgeLoader has more than one Endpoint.
   */
  protected void waitForClientToFullyConnect(final PoolImpl pool) {
    getLogWriter().info("[waitForClientToFullyConnect]");
    final long failMillis = System.currentTimeMillis() + JOIN_FAIL_MILLIS;
    boolean fullyConnected = false;
    while (!fullyConnected) {
      pause(100);
      fullyConnected = pool.getConnectionCount() >= pool.getMinConnections();
      assertTrue("Client failed to create "
                 + pool.getMinConnections()
                 + " connections within " + JOIN_FAIL_MILLIS
                 + " milliseconds. Only " + pool.getConnectionCount()
                 + " connections were created.",
                 System.currentTimeMillis() < failMillis);
    }
    getLogWriter().info("[waitForClientToFullyConnect] fullyConnected=" + fullyConnected);
  }
  
  /**
   * Resets all elements of arrays used for listener testing. Boolean values
   * are reset to false. String values are reset to null.
   */
  private void resetArraysForTesting(boolean[] fired, 
                                     DistributedMember[] member,
                                     String[] memberId, 
                                     boolean[] isClient) {
    for (int i = 0; i < fired.length; i++) {
      fired[i] = false;
      member[i] = null;
      memberId[i] = null;
      isClient[i] = false;
    }
  }

  /**
   * Asserts all elements in the array are false.
   */
  private void assertFalse(boolean[] array) {
    assertFalse(null, array);
  }
  private void assertFalse(String msg, boolean[] array) {
    for (int i = 0; i < array.length; i++) {
      if (msg == null) {
        assertFalse(array[i]);
      } else {
        assertFalse(msg, array[i]);
      }
    }    
  }
  private void assertTrue(boolean[] array) {
    assertTrue(null, array);
  }
  private void assertTrue(String msg, boolean[] array) {
    for (int i = 0; i < array.length; i++) {
      if (msg == null) {
        assertTrue(array[i]);
      } else {
        assertTrue(msg, array[i]);
      }
    }    
  }
  
  /**
   * Tests notification of events for bridge server in system bridge client
   * process.
   */
  public void testServerEventsInSystemClient() throws Exception {
     // TODO: remove for-loop when bug 37482 is fixed
     for (int i = 0; i < 3; i++) {
       try {
         doTestServerEventsInSystemClient();
         break;
       }
       catch (AssertionFailedError e) {
         // probably hit bug 37482; retry up to 3 times
         if (i >= 3) {
           throw e;
         }
         getLogWriter().info("testServerEventsInSystemClient about to repeat " +
            "after attempt " + i +  " threw AFE (see bug 37482).");
       }
       finally {
         disconnectAllFromDS();
       }
     }
  }
  protected static int testServerEventsInSystemClient_port;
  private static int getTestServerEventsInSystemClient_port() {
    return testServerEventsInSystemClient_port;
  }
  private void doTestServerEventsInSystemClient() throws Exception {
    // KIRK: this test fails intermittently with bug 37482
    final boolean[] firedSystem = new boolean[3];
    final DistributedMember[] memberSystem = new DistributedMember[3];
    final String[] memberIdSystem = new String[3];
    final boolean[] isClientSystem = new boolean[3];

    final boolean[] firedAdapter = new boolean[3];
    final DistributedMember[] memberAdapter = new DistributedMember[3];
    final String[] memberIdAdapter = new String[3];
    final boolean[] isClientAdapter = new boolean[3];

    final boolean[] firedBridge = new boolean[3];
    final DistributedMember[] memberBridge = new DistributedMember[3];
    final String[] memberIdBridge = new String[3];
    final boolean[] isClientBridge = new boolean[3];

    final boolean[] firedSystemDuplicate = new boolean[3];
    final boolean[] firedAdapterDuplicate = new boolean[3];
    final boolean[] firedBridgeDuplicate = new boolean[3];
    
    SystemMembershipListener systemListener = new SystemMembershipListener() {
      public synchronized void memberJoined(SystemMembershipEvent event) {
        firedSystemDuplicate[JOINED] = firedSystem[JOINED];
        firedSystem[JOINED] = true;
        memberSystem[JOINED] = event.getDistributedMember();
        memberIdSystem[JOINED] = event.getMemberId();
        notify();
      }
      public synchronized void memberLeft(SystemMembershipEvent event) {
        firedSystemDuplicate[LEFT] = firedSystem[LEFT];
        firedSystem[LEFT] = true;
        memberSystem[LEFT] = event.getDistributedMember();
        memberIdSystem[LEFT] = event.getMemberId();
        notify();
      }
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        firedSystemDuplicate[CRASHED] = firedSystem[CRASHED];
        firedSystem[CRASHED] = true;
        memberSystem[CRASHED] = event.getDistributedMember();
        memberIdSystem[CRASHED] = event.getMemberId();
        notify();
      }
    };

    UniversalMembershipListenerAdapter adapter = 
      new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(SystemMembershipEvent event) {
        getLogWriter().info("[testServerEventsInSystemClient] memberJoined >" + event.getMemberId() + "<");
        firedAdapterDuplicate[JOINED] = firedAdapter[JOINED];
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[JOINED] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
      @Override
      public synchronized void memberLeft(SystemMembershipEvent event) {
        getLogWriter().info("[testServerEventsInSystemClient] memberLeft >" + event.getMemberId() + "<");
        firedAdapterDuplicate[LEFT] = firedAdapter[LEFT];
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[LEFT] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
      @Override
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        getLogWriter().info("[testServerEventsInSystemClient] memberCrashed >" + event.getMemberId() + "<");
        firedAdapterDuplicate[CRASHED] = firedAdapter[CRASHED];
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[CRASHED] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
    };

    BridgeMembershipListener bridgeListener = new BridgeMembershipListener() {
      public synchronized void memberJoined(BridgeMembershipEvent event) {
        firedBridgeDuplicate[JOINED] = firedBridge[JOINED];
        firedBridge[JOINED] = true;
        memberBridge[JOINED] = event.getMember();
        memberIdBridge[JOINED] = event.getMemberId();
        isClientBridge[JOINED] = event.isClient();
        notify();
      }
      public synchronized void memberLeft(BridgeMembershipEvent event) {
        firedBridgeDuplicate[LEFT] = firedBridge[LEFT];
        firedBridge[LEFT] = true;
        memberBridge[LEFT] = event.getMember();
        memberIdBridge[LEFT] = event.getMemberId();
        isClientBridge[LEFT] = event.isClient();
        notify();
      }
      public synchronized void memberCrashed(BridgeMembershipEvent event) {
        firedBridgeDuplicate[CRASHED] = firedBridge[CRASHED];
        firedBridge[CRASHED] = true;
        memberBridge[CRASHED] = event.getMember();
        memberIdBridge[CRASHED] = event.getMemberId();
        isClientBridge[CRASHED] = event.isClient();
        notify();
      }
    };
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[] 
      { AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) };
    assertTrue(ports[0] != 0);

//    getLogWriter().info("[testServerEventsInSystemClient] create bridge client");
//    Properties config = new Properties();
//    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
//    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
//    getSystem(config);
//    AttributesFactory factory = new AttributesFactory();
//    factory.setScope(Scope.LOCAL);
//    BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
//    createRegion(name, factory.create());
//    assertNotNull(getRootRegion().getSubregion(name));
        
    getLogWriter().info("[testServerEventsInSystemClient] create system bridge client");
    getSystem();
    final Properties clientProperties = getSystemProperties();

    // register the bridge listener
    BridgeMembership.registerBridgeMembershipListener(bridgeListener);
    
    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), null);
    AdminDistributedSystem adminDS = 
      AdminDistributedSystemFactory.getDistributedSystem(config);
    adminDS.connect();
    assertTrue(adminDS.waitToBeConnected(5 * 1000));

    // register the system listener
    adminDS.addMembershipListener(systemListener);

    // register the universal adapter
    adapter.registerMembershipListener(adminDS);

    // create BridgeServer in vm0...
    SerializableRunnable createBridgeServer =
    new CacheSerializableRunnable("Create BridgeServer") {
      @Override
      public void run2() throws CacheException {
        getLogWriter().info("[testServerEventsInSystemClient] Create BridgeServer");
        getSystem(clientProperties);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertNotNull(region);
        assertNotNull(getRootRegion().getSubregion(name));
        try {
          testServerEventsInSystemClient_port = startBridgeServer(ports[0]);
        }
        catch (IOException e) {
          getLogWriter().error(e);
          fail(e.getMessage());
        }
      }
    };
    
    vm0.invoke(createBridgeServer);
    
    // gather details for later creation of BridgeLoader...
    assertEquals(ports[0],
                 vm0.invokeInt(UniversalMembershipListenerAdapterDUnitTest.class, 
                               "getTestServerEventsInSystemClient_port"));
    String serverMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    DistributedMember serverMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");

    getLogWriter().info("[testServerEventsInSystemClient] ports[0]=" + ports[0]);
    getLogWriter().info("[testServerEventsInSystemClient] serverMemberId=" + serverMemberId);
    getLogWriter().info("[testServerEventsInSystemClient] serverMember=" + serverMember);

    // create region which connects to bridge server
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
    createRegion(name, factory.create());
    assertNotNull(getRootRegion().getSubregion(name));

    synchronized(systemListener) {
      if (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testServerEventsInSystemClient] assert client detected server join");
    assertFalse(firedSystemDuplicate);
    // TODO: sometimes get adapter duplicate since memberId isn't endpoint
    // initial impl uses Endpoint.toString() for memberId of server; final
    // impl should have server send its real memberId to client via HandShake
    //assertFalse("Please update testBridgeMembershipEventsInClient to use BridgeServer memberId.",
    //           firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertEquals(serverMember, memberBridge[JOINED]);
    assertEquals(serverMemberId, memberIdBridge[JOINED]);
    assertNotNull(memberBridge[JOINED]);
    assertNotNull(memberIdBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertTrue(firedSystem[JOINED]);
    assertEquals(serverMember, memberSystem[JOINED]);
    assertEquals(serverMemberId, memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertNotNull(memberAdapter[JOINED]);
    assertNotNull(memberIdAdapter[JOINED]);
    assertEquals(serverMember, memberAdapter[JOINED]);
    assertEquals(serverMemberId, memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    getLogWriter().info("[testServerEventsInSystemClient] wait for client to fully connect");
    final String pl =
      getRootRegion().getSubregion(name).getAttributes().getPoolName();
    PoolImpl pi = (PoolImpl)PoolManager.find(pl);
    waitForClientToFullyConnect(pi);
    
    LogWriter bgexecLogger =
          new LocalLogWriter(LogWriterImpl.ALL_LEVEL, System.out);
    bgexecLogger.info("<ExpectedException action=add>" + 
        "java.io.IOException" + "</ExpectedException>");
    final ExpectedException ex = addExpectedException(
        ServerConnectivityException.class.getName());
    try {
      vm0.invoke(new SerializableRunnable("Disconnect bridge server") {
        public void run() {
          getLogWriter().info("[testServerEventsInSystemClient] disconnect bridge server");
          closeCache();
          disconnectFromDS();
        }
      });
  
      synchronized(systemListener) {
        if (!firedSystem[LEFT]) {
          systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
      synchronized(adapter) {
        if (!firedAdapter[LEFT]) {
          adapter.wait(ASYNC_EVENT_WAIT_MILLIS); // KIRK: did increasing this solve problem on balrog?
        }
      }
      synchronized(bridgeListener) {
        if (!firedBridge[LEFT]) {
          bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
    }
    finally {
      bgexecLogger.info("<ExpectedException action=remove>" + 
          "java.io.IOException" + "</ExpectedException>");
      ex.remove();
    }

    getLogWriter().info("[testServerEventsInSystemClient] assert client detected server crashed");
    assertFalse(firedSystemDuplicate);
    // TODO: sometimes get adapter duplicate since memberId isn't endpoint
    // initial impl uses Endpoint.toString() for memberId of server; final
    // impl should have server send its real memberId to client via HandShake
    //assertFalse("Please update testBridgeMembershipEventsInClient to use BridgeServer memberId.",
    //           firedAdapterDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertFalse(firedBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse("Please update testServerEventsInSystemClient to handle memberLeft for BridgeServer.", 
                firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertTrue(firedBridge[CRASHED]);
    assertNotNull(memberBridge[CRASHED]);
    assertNotNull(memberIdBridge[CRASHED]);
    assertEquals(serverMemberId, memberIdBridge[CRASHED]);
    assertEquals(serverMember, memberBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedSystem[JOINED]);
    assertNull(memberSystem[JOINED]);
    assertNull(memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertTrue(firedSystem[LEFT]);
    assertEquals(serverMember, memberSystem[LEFT]);
    assertEquals(serverMemberId, memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertFalse("this intermittently fails", firedAdapter[JOINED]); // KIRK --> this fails on balrog occasionally
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    // LEFT fired by System listener
    assertTrue(firedAdapter[LEFT]);
    assertEquals(serverMember, memberAdapter[LEFT]);
    assertEquals(serverMemberId, memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);

    // There won't be an adapter crashed event because since the two VMs
    // are in the same distributed system, and the server's real member
    // id is used now. In this case, two events are sent - one from
    // jgroups (memberDeparted), and one from the server (a memberCrshed).
    // The memberCrashed event is deemed a duplicate and not sent - see 
    // UniversalMembershipListenerAdapter.SystemMembershipListener.isDuplicate
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    // reconnect bridge client to test for crashed event
    vm0.invoke(createBridgeServer);
    
    // gather details for later creation of BridgeLoader...
    assertEquals(ports[0],
                 vm0.invokeInt(UniversalMembershipListenerAdapterDUnitTest.class, 
                               "getTestServerEventsInSystemClient_port"));
    serverMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    serverMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");

    getLogWriter().info("[testServerEventsInSystemClient] ports[0]=" + ports[0]);
    getLogWriter().info("[testServerEventsInSystemClient] serverMemberId=" + serverMemberId);
    getLogWriter().info("[testServerEventsInSystemClient] serverMember=" + serverMember);
                                                
    synchronized(systemListener) {
      if (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testServerEventsInSystemClient] assert client detected server re-join");
    assertFalse(firedSystemDuplicate);
    // TODO: sometimes get adapter duplicate since memberId isn't endpoint
    // initial impl uses Endpoint.toString() for memberId of server; final
    // impl should have server send its real memberId to client via HandShake
    //assertFalse("Please update testBridgeMembershipEventsInClient to use BridgeServer memberId.",
    //           firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertNotNull(memberBridge[JOINED]);
    assertNotNull(memberIdBridge[JOINED]);
    assertEquals(serverMember, memberBridge[JOINED]);
    assertEquals(serverMemberId, memberIdBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertTrue(firedSystem[JOINED]);
    assertEquals(serverMember, memberSystem[JOINED]);
    assertEquals(serverMemberId, memberIdSystem[JOINED]);
    assertFalse(isClientSystem[JOINED]);
    assertFalse(firedSystem[LEFT]);
    assertNull(memberSystem[LEFT]);
    assertNull(memberIdSystem[LEFT]);
    assertFalse(isClientSystem[LEFT]);
    assertFalse(firedSystem[CRASHED]);
    assertNull(memberSystem[CRASHED]);
    assertNull(memberIdSystem[CRASHED]);
    assertFalse(isClientSystem[CRASHED]);
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertTrue(firedAdapter[JOINED]);
    assertNotNull(memberAdapter[JOINED]);
    assertNotNull(memberIdAdapter[JOINED]);
    assertEquals(serverMember, memberAdapter[JOINED]);
    assertEquals(serverMemberId, memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
  }

  /**
   * Tests notification of events for bridge server in system bridge client
   * process.
   */
  public void testServerEventsInLonerClient() throws Exception {
     try {
       doTestServerEventsInLonerClient();
     }
     finally {
       disconnectAllFromDS();
     }
  }
  protected static int testServerEventsInLonerClient_port;
  private static int getTestServerEventsInLonerClient_port() {
    return testServerEventsInLonerClient_port;
  }
  private void doTestServerEventsInLonerClient() throws Exception {
    final boolean[] firedAdapter = new boolean[3];
    final DistributedMember[] memberAdapter = new DistributedMember[3];
    final String[] memberIdAdapter = new String[3];
    final boolean[] isClientAdapter = new boolean[3];

    final boolean[] firedBridge = new boolean[3];
    final DistributedMember[] memberBridge = new DistributedMember[3];
    final String[] memberIdBridge = new String[3];
    final boolean[] isClientBridge = new boolean[3];

    final boolean[] firedAdapterDuplicate = new boolean[3];
    final boolean[] firedBridgeDuplicate = new boolean[3];
    
    UniversalMembershipListenerAdapter adapter = 
      new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(SystemMembershipEvent event) {
        getLogWriter().info("[testServerEventsInLonerClient] memberJoined >" + event.getMemberId() + "<");
        firedAdapterDuplicate[JOINED] = firedAdapter[JOINED];
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[JOINED] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
      @Override
      public synchronized void memberLeft(SystemMembershipEvent event) {
        getLogWriter().info("[testServerEventsInLonerClient] memberLeft >" + event.getMemberId() + "<");
        firedAdapterDuplicate[LEFT] = firedAdapter[LEFT];
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[LEFT] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
      @Override
      public synchronized void memberCrashed(SystemMembershipEvent event) {
        getLogWriter().info("[testServerEventsInLonerClient] memberCrashed >" + event.getMemberId() + "<");
        firedAdapterDuplicate[CRASHED] = firedAdapter[CRASHED];
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        if (event instanceof UniversalMembershipListenerAdapter.AdaptedMembershipEvent) {
          isClientAdapter[CRASHED] = 
            ((UniversalMembershipListenerAdapter.AdaptedMembershipEvent)event).isClient();
        }
        notify();
      }
    };

    BridgeMembershipListener bridgeListener = new BridgeMembershipListener() {
      public synchronized void memberJoined(BridgeMembershipEvent event) {
        firedBridgeDuplicate[JOINED] = firedBridge[JOINED];
        firedBridge[JOINED] = true;
        memberBridge[JOINED] = event.getMember();
        memberIdBridge[JOINED] = event.getMemberId();
        isClientBridge[JOINED] = event.isClient();
        notify();
      }
      public synchronized void memberLeft(BridgeMembershipEvent event) {
        firedBridgeDuplicate[LEFT] = firedBridge[LEFT];
        firedBridge[LEFT] = true;
        memberBridge[LEFT] = event.getMember();
        memberIdBridge[LEFT] = event.getMemberId();
        isClientBridge[LEFT] = event.isClient();
        notify();
      }
      public synchronized void memberCrashed(BridgeMembershipEvent event) {
        firedBridgeDuplicate[CRASHED] = firedBridge[CRASHED];
        firedBridge[CRASHED] = true;
        memberBridge[CRASHED] = event.getMember();
        memberIdBridge[CRASHED] = event.getMemberId();
        isClientBridge[CRASHED] = event.isClient();
        notify();
      }
    };
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[] 
      { AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) };
    assertTrue(ports[0] != 0);

    getLogWriter().info("[testServerEventsInLonerClient] create loner bridge client");
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
        
    getLogWriter().info("[testServerEventsInLonerClient] create system bridge client");
    getSystem();

    // register the bridge listener
    BridgeMembership.registerBridgeMembershipListener(bridgeListener);
    
    // adapter should've self-registered w/ BridgeMembership
    
//    String clientMemberId = getMemberId();
//    DistributedMember clientMember = getDistributedMember();
                                                
    // create BridgeServer in vm0...
    SerializableRunnable createBridgeServer =
    new CacheSerializableRunnable("Create BridgeServer") {
      @Override
      public void run2() throws CacheException {
        getLogWriter().info("[testServerEventsInLonerClient] Create BridgeServer");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertNotNull(region);
        assertNotNull(getRootRegion().getSubregion(name));
        try {
          testServerEventsInLonerClient_port = startBridgeServer(ports[0]);
        }
        catch (IOException e) {
          getLogWriter().error(e);
          fail(e.getMessage());
        }
      }
    };
    
    vm0.invoke(createBridgeServer);
    
    // gather details for later creation of BridgeLoader...
    assertEquals(ports[0],
                 vm0.invokeInt(UniversalMembershipListenerAdapterDUnitTest.class, 
                               "getTestServerEventsInLonerClient_port"));
    String serverMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    DistributedMember serverMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");

    getLogWriter().info("[testServerEventsInLonerClient] ports[0]=" + ports[0]);
    getLogWriter().info("[testServerEventsInLonerClient] serverMemberId=" + serverMemberId);
    getLogWriter().info("[testServerEventsInLonerClient] serverMember=" + serverMember);

    // create region which connects to bridge server
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    BridgeTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
    createRegion(name, factory.create());
    assertNotNull(getRootRegion().getSubregion(name));

    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testServerEventsInLonerClient] assert client detected server join");
    // TODO: sometimes get adapter duplicate since memberId isn't endpoint KIRK
    // initial impl uses Endpoint.toString() for memberId of server; final
    // impl should have server send its real memberId to client via HandShake
    //assertFalse("Please update testBridgeMembershipEventsInClient to use BridgeServer memberId.",
    //           firedAdapterDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertEquals(serverMember, memberBridge[JOINED]);
    assertEquals(serverMemberId, memberIdBridge[JOINED]);
    assertNotNull(memberBridge[JOINED]);
    assertNotNull(memberIdBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertTrue(firedAdapter[JOINED]);
    assertEquals(serverMember, memberAdapter[JOINED]);
    assertEquals(serverMemberId, memberIdAdapter[JOINED]);
    assertNotNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    getLogWriter().info("[testServerEventsInLonerClient] wait for client to fully connect");
    final String pl =
      getRootRegion().getSubregion(name).getAttributes().getPoolName();
    PoolImpl pi = (PoolImpl)PoolManager.find(pl);
    waitForClientToFullyConnect(pi);
    
    String expected = "java.io.IOException";
    String addExpected = 
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    String removeExpected = 
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    String expected2 = "java.net.ConnectException";
    String addExpected2 = 
      "<ExpectedException action=add>" + expected2 + "</ExpectedException>";
    String removeExpected2 = 
      "<ExpectedException action=remove>" + expected2 + "</ExpectedException>";
      
    //LogWriter bgexecLogger =
    //      new LocalLogWriter(LocalLogWriter.ALL_LEVEL, System.out);
    //bgexecLogger.info(addExpected);
    //bgexecLogger.info(addExpected2);
    LogWriter lw = getSystem().getLogWriter();
    lw.info(addExpected);
    lw.info(addExpected2);

    try {
      vm0.invoke(new SerializableRunnable("Disconnect bridge server") {
        public void run() {
          getLogWriter().info("[testServerEventsInLonerClient] disconnect bridge server");
          closeCache();
        }
      });
  
      synchronized(adapter) {
        if (!firedAdapter[LEFT]) {
          adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
      
      synchronized(bridgeListener) {
        if (!firedBridge[LEFT] && !firedBridge[CRASHED]) {
          bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
        }
      }
    }
    finally {
      //bgexecLogger.info(removeExpected);
      //bgexecLogger.info(removeExpected2);
      lw.info(removeExpected);
      lw.info(removeExpected2);
    }
    
    getLogWriter().info("[testServerEventsInLonerClient] assert client detected server crashed");
    // TODO: sometimes get adapter duplicate since memberId isn't endpoint KIRK
    // initial impl uses Endpoint.toString() for memberId of server; final
    // impl should have server send its real memberId to client via HandShake
    //assertFalse("Please update testBridgeMembershipEventsInClient to use BridgeServer memberId.",
    //           firedAdapterDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertFalse(firedBridge[JOINED]);
    assertNull(memberIdBridge[JOINED]);
    assertNull(memberBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse("Please update testServerEventsInLonerClient to handle memberLeft for BridgeServer.", 
                firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertTrue(firedBridge[CRASHED]);
    assertNotNull(memberBridge[CRASHED]);
    assertNotNull(memberIdBridge[CRASHED]);
    assertEquals(serverMember, memberAdapter[CRASHED]);
    assertEquals(serverMemberId, memberIdAdapter[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertFalse(firedAdapter[JOINED]);
    assertNull(memberAdapter[JOINED]);
    assertNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse("Please update testServerEventsInLonerClient to handle BridgeServer LEFT", 
                firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    // CRASHED fired by Bridge listener
    assertTrue(firedAdapter[CRASHED]);
    assertNotNull(memberAdapter[CRASHED]);
    assertNotNull(memberIdAdapter[CRASHED]);
    assertEquals(serverMember, memberAdapter[CRASHED]);
    assertEquals(serverMemberId, memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
    
    // reconnect bridge client to test for crashed event
    vm0.invoke(createBridgeServer);
    
    // gather details for later creation of BridgeLoader...
    assertEquals(ports[0],
                 vm0.invokeInt(UniversalMembershipListenerAdapterDUnitTest.class, 
                               "getTestServerEventsInLonerClient_port"));
    serverMemberId = (String) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getMemberId");
    serverMember = (DistributedMember) vm0.invoke(
      UniversalMembershipListenerAdapterDUnitTest.class, "getDistributedMember");

    getLogWriter().info("[testServerEventsInLonerClient] ports[0]=" + ports[0]);
    getLogWriter().info("[testServerEventsInLonerClient] serverMemberId=" + serverMemberId);
    getLogWriter().info("[testServerEventsInLonerClient] serverMember=" + serverMember);
                                                
    synchronized(adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized(bridgeListener) {
      if (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    
    getLogWriter().info("[testServerEventsInLonerClient] assert client detected server re-join");
    // TODO: sometimes get adapter duplicate since memberId isn't endpoint KIRK
    // initial impl uses Endpoint.toString() for memberId of server; final
    // impl should have server send its real memberId to client via HandShake
    //assertFalse("Please update testBridgeMembershipEventsInClient to use BridgeServer memberId.",
    //           firedAdapterDuplicate);
    assertFalse(firedAdapterDuplicate);
    assertFalse(firedBridgeDuplicate);

    assertTrue(firedBridge[JOINED]);
    assertNotNull(memberBridge[JOINED]);
    assertNotNull(memberIdBridge[JOINED]);
    assertEquals(serverMember, memberBridge[JOINED]);
    assertEquals(serverMemberId, memberIdBridge[JOINED]);
    assertFalse(isClientBridge[JOINED]);
    assertFalse(firedBridge[LEFT]);
    assertNull(memberBridge[LEFT]);
    assertNull(memberIdBridge[LEFT]);
    assertFalse(isClientBridge[LEFT]);
    assertFalse(firedBridge[CRASHED]);
    assertNull(memberBridge[CRASHED]);
    assertNull(memberIdBridge[CRASHED]);
    assertFalse(isClientBridge[CRASHED]);
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);
    
    assertTrue(firedAdapter[JOINED]);
    assertEquals(serverMember, memberAdapter[JOINED]);
    assertEquals(serverMemberId, memberIdAdapter[JOINED]);
    assertNotNull(memberAdapter[JOINED]);
    assertNotNull(memberIdAdapter[JOINED]);
    assertFalse(isClientAdapter[JOINED]);
    assertFalse(firedAdapter[LEFT]);
    assertNull(memberAdapter[LEFT]);
    assertNull(memberIdAdapter[LEFT]);
    assertFalse(isClientAdapter[LEFT]);
    assertFalse(firedAdapter[CRASHED]);
    assertNull(memberAdapter[CRASHED]);
    assertNull(memberIdAdapter[CRASHED]);
    assertFalse(isClientAdapter[CRASHED]);
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
  }

  // Simple DistributedMember implementation
  static final class TestDistributedMember implements DistributedMember {
    
    private final String host;
    
    public TestDistributedMember(String host) {
      this.host = host;
}

    public String getName() {
      return "";
    }

    public String getHost() {
      return this.host;
    }

    public Set getRoles() {
      return new HashSet();
    }

    public int getProcessId() {
      return 0;
    }

    public String getId() {
      return this.host;
    }
    
    public int compareTo(DistributedMember o) {
      if ((o == null) || !(o instanceof TestDistributedMember)) {
        throw new InternalGemFireException("Invalidly comparing TestDistributedMember to " + o);
      }
      
      TestDistributedMember tds = (TestDistributedMember) o;
      return getHost().compareTo(tds.getHost());
    }
    
    @Override
    public boolean equals(Object obj) {
      if ((obj == null) || !(obj instanceof TestDistributedMember)) {
        return false;
      }
      return compareTo((TestDistributedMember)obj) == 0;
    }
    
    @Override
    public int hashCode() {
      return getHost().hashCode();
    }
    
    public DurableClientAttributes getDurableClientAttributes() {
     return null;
    }

    public List<String> getGroups() {
      return Collections.emptyList();
    }
  }

}

