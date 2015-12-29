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
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import java.util.ArrayList;
import dunit.*;

/**
 * Tests the admin SystemMemberCacheListener.
 *
 * @author Darrel Schneider
 * @since 5.0
 */
public class SystemMemberCacheListenerDUnitTest extends AdminDUnitTestCase {

  protected volatile int listenerInvoked = 0;
  protected volatile boolean createInvoked = false;
  protected volatile boolean lossInvoked = false;
  protected transient volatile SystemMemberRegionEvent lastEvent = null;
  protected transient volatile SystemMemberCacheEvent lastCacheEvent = null;
  private transient DistributedMember otherId;
  
  private void clearEventState() {
    synchronized (this) {
      this.listenerInvoked = 0;
      this.createInvoked = false;
      this.lossInvoked = false;
      this.lastEvent = null;
      this.lastCacheEvent = null;
    }
  }

  private void assertNoEventState() {
    synchronized (this) {
      assertEquals(0, this.listenerInvoked);
      assertEquals(null, this.lastEvent);
      assertEquals(null, this.lastCacheEvent);
    }
  }

  protected int getListenerInvoked() {
    synchronized (this) {
      return this.listenerInvoked;
    }
  }
  
  static private final String WAIT_PROPERTY = 
    "SystemMemberCacheListenerDUnitTest.maxWaitTime";
  static private final int WAIT_DEFAULT = 40000;
  

  private void waitForListenerInvoke(final int expected) throws Exception {
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();

    // wait for up to 5000 ms for listener invoke
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return getListenerInvoked() >= expected;
      }
      public String description() {
        return "listener invocations never reached " + expected;
      }
    };
    DistributedTestCase.waitForCriterion(ev, maxWaitTime, 200, true);
    
    // while-loop may only exit without error if invoked is >= expected
    // fail assertion if invoked is > expected
    assertEquals("waitForListenerInvoke listener invoked not equal to expected", 
                 expected, getListenerInvoked());
  }
  
  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }
    
  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
        public void run2() throws CacheException {
          getCache();
          // force communication flush to make sure the cache_create event
          // is distributed before returning
          //sendUnorderedMessageToAdminMembers();
        }
      });
    this.otherId = (DistributedMember)vm.invoke(SystemMemberCacheListenerDUnitTest.class, 
                                                "getVMDistributedMember");
  }
  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }

  /**
   * Creates a new <code>SystemMemberRegionDUnitTest</code>
   */
  public SystemMemberCacheListenerDUnitTest(String name) {
    super(name);
  }
  

  private void createRegionInVM(final String name, final VM vm) throws Exception {
    vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        AttributesFactory af = new AttributesFactory();
        createRootRegion(name, af.create());
      }
    });
    waitForListenerInvoke(1);
    synchronized (this) {
      assertEquals(null, this.lastCacheEvent);
      assertEquals(Operation.REGION_CREATE, this.lastEvent.getOperation());
      assertEquals("/" + name, this.lastEvent.getRegionPath());
      assertEquals(this.otherId, this.lastEvent.getDistributedMember());
      assertEquals(true, this.createInvoked);
    }
    clearEventState();
  }

  ////////  Test Methods

  public static DistributedMember getMemberId() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
  
  /**
   * Tests that admin region listener gets events on region create/destroy
   */
  public void testCacheListener() throws Exception {
    clearEventState();

    initOtherId();
    VM cacheVM = getOtherVm();

    ArrayList ids = new ArrayList();
    ids.add(cacheVM.invoke(SystemMemberCacheListenerDUnitTest.class, "getMemberId"));
    

    TestSystemMemberCacheListener cacheListener = 
        new TestSystemMemberCacheListener(ids);

    //System.out.println("DEBUG before addCacheListener"); System.out.flush();
    this.tcSystem.addCacheListener(cacheListener);
    // wait for up to 5000 ms for listener invoke for the create cache event.
    // it is possible we will miss this because it arrives before the listener.
    {
      long endTime = System.currentTimeMillis() + 5000;
      while (getListenerInvoked() == 0 && System.currentTimeMillis() > endTime) {
        pause(100);
      }
      if (getListenerInvoked() != 0) {
        clearEventState();
      }
    }
    assertNoEventState();

    /* 
          vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        AttributesFactory af = new AttributesFactory();
        createRootRegion(name, af.createRegionAttributes());
      }
    });

     */
    // need to do the following hasCache and getCache in vm instead of controller
//     if (!hasCache()) {
//       getCache();
//       waitForListenerInvoke(1);
//       synchronized (this) {
//         assertEquals(null, this.lastEvent);
//         assertEquals(Operation.CACHE_CREATE, this.lastCacheEvent.getOperation());
//         assertEquals(this.otherId, this.lastCacheEvent.getDistributedMember());
//       }
//       clearEventState();
//     }

    createRegionInVM("r1", cacheVM);
    cacheVM.invoke(new CacheSerializableRunnable("destroy region") {
      public void run2() throws CacheException {
        getRootRegion("r1").destroyRegion();
      }
    });
    waitForListenerInvoke(1);
    synchronized (this) {
      assertEquals(true, this.lossInvoked);
      assertEquals(Operation.REGION_DESTROY, this.lastEvent.getOperation());
      assertEquals("/r1", this.lastEvent.getRegionPath());
      assertEquals(this.otherId, this.lastEvent.getDistributedMember());
    }
    clearEventState();

    createRegionInVM("r2", cacheVM);
    cacheVM.invoke(new CacheSerializableRunnable("local destroy region") {
      public void run2() throws CacheException {
        getRootRegion("r2").localDestroyRegion();
      }
    });
    waitForListenerInvoke(1);
    synchronized (this) {
      assertEquals(true, this.lossInvoked);
      assertEquals(Operation.REGION_LOCAL_DESTROY, this.lastEvent.getOperation());
      assertEquals("/r2", this.lastEvent.getRegionPath());
      assertEquals(this.otherId, this.lastEvent.getDistributedMember());
    }
    clearEventState();

    createRegionInVM("r3", cacheVM);
    cacheVM.invoke(new CacheSerializableRunnable("close region") {
      public void run2() throws CacheException {
        getRootRegion("r3").close();
      }
    });
    waitForListenerInvoke(1);
    synchronized (this) {
      assertEquals(true, this.lossInvoked);
      assertEquals(Operation.REGION_CLOSE, this.lastEvent.getOperation());
      assertEquals("/r3", this.lastEvent.getRegionPath());
      assertEquals(this.otherId, this.lastEvent.getDistributedMember());
    }
    clearEventState();

    createRegionInVM("r4", cacheVM);
    cacheVM.invoke(new CacheSerializableRunnable("close cache") {
      public void run2() throws CacheException {
        Cache cache = basicGetCache();
        assertTrue(cache != null && !cache.isClosed());
        closeCache();
      }
    });
    waitForListenerInvoke(2);
    synchronized (this) {
      assertEquals(true, this.lossInvoked);
      assertEquals(Operation.CACHE_CLOSE, this.lastEvent.getOperation());
      assertEquals("/r4", this.lastEvent.getRegionPath());
      assertEquals(this.otherId, this.lastEvent.getDistributedMember());
      assertEquals(Operation.CACHE_CLOSE, this.lastCacheEvent.getOperation());
      assertEquals(this.otherId, this.lastCacheEvent.getDistributedMember());
    }
    clearEventState();

    cacheVM.invoke(new CacheSerializableRunnable("create cache") {
      public void run2() throws CacheException {
        getCache();
      }
    });
    waitForListenerInvoke(1);
    synchronized (this) {
      assertEquals(null, this.lastEvent);
      assertEquals(Operation.CACHE_CREATE, this.lastCacheEvent.getOperation());
      assertEquals(this.otherId, this.lastCacheEvent.getDistributedMember());
    }
    clearEventState();
  }
  
  /**
   * Used to test admin region events
   */
  class TestSystemMemberCacheListener implements SystemMemberCacheListener
  {
    final ArrayList ids;

    /** Creates a new instance of TestSystemMemberCacheListener */
    public TestSystemMemberCacheListener(ArrayList ids) {
      this.ids = ids;
    }

    public void afterRegionCreate(SystemMemberRegionEvent event) {
      if (!this.ids.contains(event.getDistributedMember())) {
        return;
      }
      synchronized (SystemMemberCacheListenerDUnitTest.this) {
        listenerInvoked += 1;
        createInvoked = true;
        lastEvent = event;
      }
      getLogWriter().info("DEBUG afterRegionCreate=" + event);
    }

    public void afterRegionLoss(SystemMemberRegionEvent event) {
      if (!this.ids.contains(event.getDistributedMember())) {
        return;
      }
      synchronized (SystemMemberCacheListenerDUnitTest.this) {
        listenerInvoked += 1;
        lossInvoked = true;
        lastEvent = event;
      }
      //System.out.println("DEBUG afterRegionLoss=" + event); System.out.flush();
    }
    public void afterCacheCreate(SystemMemberCacheEvent event) {
      if (!this.ids.contains(event.getDistributedMember())) {
        return;
      }
      synchronized (SystemMemberCacheListenerDUnitTest.this) {
        listenerInvoked += 1;
        lastCacheEvent = event;
      }
      getLogWriter().info("DEBUG afterCacheCreate=" + event);
    }
    public void afterCacheClose(SystemMemberCacheEvent event) {
      if (!this.ids.contains(event.getDistributedMember())) {
        return;
      }
      synchronized (SystemMemberCacheListenerDUnitTest.this) {
        listenerInvoked += 1;
        lastCacheEvent = event;
      }
      //System.out.println("DEBUG afterCacheClose=" + event); System.out.flush();
    }
  }
}

