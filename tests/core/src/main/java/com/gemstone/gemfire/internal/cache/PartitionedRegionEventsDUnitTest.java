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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;

import dunit.*;
import java.io.Serializable;
import com.gemstone.gemfire.*;

/**
 * This class tests event triggering and handling in partitioned
 * regions.  Most of this class is a copy of CacheEventListenerTest
 *
 * @author Bruce Schuchardt
 *
 * @since 5.1
 */
public class PartitionedRegionEventsDUnitTest extends DistributedCacheTestCase {
  
  static final String REGION_NAME = "PREventsDUnitTest";
  
  volatile static Object newValue, oldValue;
  
  /**
   * flag in callback indicating test failure
   */
  static volatile boolean callbackFailure;
  
  volatile int expectedCreateInv;
  volatile int expectedUpdateInv;
    
  public PartitionedRegionEventsDUnitTest(String name) {
    super(name);
  }

  protected static void callbackAssertEquals(String message, Object expected, 
      Object actual) {
    if (expected == null && actual == null)
      return;
    if (expected != null && expected.equals(actual))
      return;
    callbackFailure = true;
    // Throws an error that is ignored, but...
    assertEquals(message, expected, actual);
  }
  
  protected static void callbackAssertTrue(String msg, boolean cond) {
    if (cond)
      return;
    callbackFailure = true;
    // Throws ignored error, but...
    assertTrue(msg, cond);
  }
  
  protected static void callbackAssertNull(String msg, Object val) {
    if (val == null)
      return;
    callbackFailure = true;
    // Throws ignored exception
    assertNull(msg, val);
  }
  /////////  Public test methods

  public void testObjectAddedReplaced()
    throws CacheException, InterruptedException {

    String name = "testObjectAddedReplaced";
    int numVMs = Host.getHost(0).getVMCount() + 1 /* controller */;
    getBlackboard().initNumInvocations();
    callbackFailure = false;

    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        getLogWriter().info("Invoking afterCreate on listener; event=" +
                            event);
        callbackAssertEquals("Callback arguments not equal",
            event.getCallbackArgument(), event.getDistributedMember());
        callbackAssertEquals("Operation not a create",
            Operation.CREATE, event.getOperation());
//        if ( ((com.gemstone.gemfire.internal.cache.EntryEventImpl)event).getIsBucketOwner() ) {
//          Assert.assertTrue(event.getOldValue() == null, 
//              "oldValue not null: " + event.getOldValue());
//        }
        getBlackboard().incNumInvocations1();
        callbackAssertEquals("newValue is wrong", new Integer(0), 
                     event.getNewValue());
        callbackAssertEquals("oldValue not null: " + event.getOldValue(), 
            null, event.getOldValue());
      }

      
      public void afterUpdate(EntryEvent event) {
        getLogWriter().info("Invoking afterUpdate on listener; name="
                            + event.getKey());
        callbackAssertEquals("Callback argument incorrect",
            event.getCallbackArgument(), event.getDistributedMember());
        callbackAssertEquals("Operation not update",
            Operation.UPDATE, event.getOperation());
        if ( ((com.gemstone.gemfire.internal.cache.EntryEventImpl)event).getInvokePRCallbacks() ) {
          callbackAssertTrue("Old value should not be null",
              event.getOldValue() != null);
        }
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
        getBlackboard().incNumInvocations2();
        getLogWriter().info("update event new value is: " + newValue );
        getLogWriter().info("update event old value is: " + oldValue );
      } 
    };

    Object[] args = new Object[] { name, new SubscriptionAttributes(InterestPolicy.ALL), listener };
    
    expectedCreateInv = 1;
    expectedUpdateInv = 0;
    
    // Create in controller VM
    LogWriter log = getLogWriter();
    log.info("TESTOBJECTADDEDREPLACED: creating entry in controller");
    createEntry(name, (SubscriptionAttributes)args[1], listener);
    log.info("TESTOBJECTADDEDREPLACED: done creating entry in controller");
    RepeatableRunnable verify = new CacheSerializableRunnable("verify") {
      public void run2() {
        assertEquals("Wrong number of 'afterCreate' listener invocations",
                     expectedCreateInv, getBlackboard().getNumInvocations1());
        assertEquals("Wrong number of 'afterUpdate' listener invocations", 
                     expectedUpdateInv, getBlackboard().getNumInvocations2()); 
      }
    };
    log.info("TESTOBJECTADDEDREPLACED: verifying");

    verify.runRepeatingIfNecessary(5000);

    log.info("TESTOBJECTADDEDREPLACED: done verifying");
    
    // Create in other VMs

    // the expected number of update events is dependent on the number of VMs.
    // Each "createEntry" does a put which fires a update event in each of
    // the other VMs that have that entry already defined at that time.
    // The first createEntry causes 0 update events.
    // The second createEntry causes 1 update events (in the first VM)
    // The third createEntry causes 2 update events, etc (in the first 2 VMs)
    Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    // since the controller has a cache and listener, expect this:
    //i            0  1  2  3
    //updates   0 +2 +3 +4 +5  (i+2)
    //          0  2  5  9 14
    for (int i=0; i < vmCount; i++) {
      log.info("TESTOBJECTADDEDREPLACED: invoking update in VM " + i);
      host.getVM(i).invoke(this.getClass(), "updateEntry", args);
      log.info("TESTOBJECTADDEDREPLACED: done with update in VM " + i);
      expectedUpdateInv += (i+2);  // with InterestPolicy.ALL, all listeners should be invoked
      log.info("TESTOBJECTADDEDREPLACED: verifying callbacks for VM " + i);
      verify.runRepeatingIfNecessary(5000);
      log.info("TESTOBJECTADDEDREPLACED: done verifying callbacks for VM " + i);
    }
    
    // getNewValue is Integer(0)
    assertEquals("newValue is wrong", new Integer(0), newValue);
    // getOldValue is NOT AVAILABLE
    //assertEquals("oldValue is wrong", CacheEvent.NOT_AVAILABLE, oldValue);

    // test replace events
    int replaceValue = 1;

    //log.info("Number of updates before replaceEntry is " + getBlackboard().getNumInvocations2());
    replaceEntry(name, new Integer (replaceValue));
    //log.info("Number of updates after replaceEntry is " + getBlackboard().getNumInvocations2());
    expectedUpdateInv += numVMs;
    verify.runRepeatingIfNecessary(5000);

    //assertEquals("oldValue is wrong", CacheEvent.NOT_AVAILABLE, getOldValue());
    assertEquals("newValue is wrong", new Integer(replaceValue), getNewValue());
    for (int i=0; i < vmCount; i++) {
      //Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      //assertEquals("oldValue is wrong in vm " + i, new Integer(replaceValue-1), vmOldValue);
      assertEquals("newValue is wrong in vm " + i, new Integer(replaceValue), vmNewValue);
    }

    replaceValue++;
    host.getVM(0).invoke(this.getClass(), "replaceEntry", new Object[] { name, new Integer(replaceValue)});
    expectedUpdateInv += numVMs;
    verify.runRepeatingIfNecessary(5000);

    //assertEquals("oldValue is wrong", new Integer(replaceValue-1), getOldValue());
    try {
      assertEquals("newValue is wrong", new Integer(replaceValue), getNewValue());
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch ( Error e ) {
      getLogWriter().severe( e );
      throw e;
    }
    for (int i=0; i < vmCount; i++) {
      //Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      //assertEquals("oldValue is wrong in vm " + i, new Integer(replaceValue-1), vmOldValue);
      assertEquals("newValue is wrong in vm " + i, new Integer(replaceValue), vmNewValue);
    }
    
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }

  public void testUpdateIsCreate()  throws CacheException, InterruptedException {

    String name = "testUpdateIsCreate";
    getBlackboard().initNumInvocations();
    callbackFailure = false;
  
    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        getLogWriter().info("Invoking afterCreate on listener; event=" +
                            event);
        callbackAssertEquals("Wrong callback argument",
            event.getCallbackArgument(), event.getDistributedMember());
        callbackAssertEquals("Operation not create",
            Operation.CREATE, event.getOperation());
        if ( !((com.gemstone.gemfire.internal.cache.EntryEventImpl)event).getInvokePRCallbacks() ) {
          callbackAssertEquals("Old value should be NOT_AVAILABLE",
             null, event.getOldValue());
          assertEquals("Old value should be NOT_AVAILABLE",
                       false, event.isOldValueAvailable());
        }
        else {
          callbackAssertNull("the oldValue should be null", event.getOldValue());
        }
        getBlackboard().incNumInvocations1();
        callbackAssertEquals("newValue is wrong", new Integer(0), 
                     event.getNewValue());
        //assertNull("oldValue not null", event.getOldValue());
      }
  
      
      public void afterUpdate(EntryEvent event) {
        getLogWriter().info("Invoking afterUpdate on listener; name="
                            + event.getKey());
        callbackAssertEquals("Operation not an update",
            Operation.UPDATE, event.getOperation());
        getBlackboard().incNumInvocations2();
      } 
    };
  
    Object[] args = new Object[] { name, new SubscriptionAttributes(InterestPolicy.ALL), listener };
    
    // Create in controller vm
    LogWriter log = getLogWriter();
    log.info(name +": creating region in controller");
    createRegion(name, (SubscriptionAttributes)args[1], listener);
    log.info(name +": done creating region in controller");
  
    // Create in VMs 1-(vmCount-1)
  
    Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int i=1; i < vmCount; i++) {
      log.info(name +": invoking region creation in VM " + i);
      host.getVM(i).invoke(this.getClass(), "createRegion", args);
      log.info(name +": done with region creation in VM " + i);
    }
    
    expectedCreateInv = vmCount+1;
    expectedUpdateInv = 0;
    
    host.getVM(0).invoke(this.getClass(), "updateNewEntry", args);
    
    assertEquals("wrong number of 'afterUpdate' listener invocations",
        expectedUpdateInv, getBlackboard().getNumInvocations2());
    assertEquals("wrong number of 'afterCreate' listener invocations",
        expectedCreateInv, getBlackboard().getNumInvocations1());
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }

  public void testObjectInvalidated()
    throws CacheException, InterruptedException {

    int ttl = 0;  // 5;  // seconds  expiration isn't supported in PR (yet??)
    String name = "testObjectInvalidated";
    Host host = Host.getHost(0);
    final int numVMs = host.getVMCount() + 1 /* controller */;
    getBlackboard().initNumInvocations();
    callbackFailure = false;

    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        // ignore
      }
      public void afterInvalidate(EntryEvent event) {
        getLogWriter().info("Invoking tests invalidated listener");
        if (event.isOriginRemote()) {
          callbackAssertTrue("Wrong distributed member",
              !event.getDistributedMember().equals(
                  getSystem().getDistributedMember()));
        } else {
          callbackAssertEquals("Wrong distributed member",
              getSystem().getDistributedMember(), 
              event.getDistributedMember());
        }
        callbackAssertEquals("Wrong operation",
            Operation.INVALIDATE, event.getOperation());
        getBlackboard().incNumInvocations1();
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
      }
      
      public void afterUpdate(EntryEvent event) {
        // ignore
      }
    };

    Object[] args = new Object[] { 
      name, new SubscriptionAttributes(InterestPolicy.ALL), 
      listener};
    
    // Create in controller VM
    createEntry(name, ttl, ExpirationAction.INVALIDATE, new SubscriptionAttributes(InterestPolicy.ALL), listener);
    // create region and listeners in other VMs
    forEachVMInvoke("createSubregion", args);
    
    // Wait for expiration
    //getLogWriter().info("Sleeping for " + ttl + " seconds");
    //Thread.sleep(ttl * 1000);
    
    // Invalidate the entry
    Region region = getRegion();
    Region sub = region.getSubregion(name);
    assertNotNull(sub);
    getLogWriter().info("About to invalidate " + name);
    sub.invalidate(name);
    

    // invoked at least once
    // invoked no more than once
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return getBlackboard().getNumInvocations1() >= numVMs;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

    assertNull(getRegion().getSubregion(name).get(name));

    assertNull(newValue);
    //assertEquals(new Integer(0), oldValue);

    for (int i=0; i < host.getVMCount(); i++) {
      //Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      //assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }

  public void testObjectDestroyed()
    throws CacheException, InterruptedException {

    int ttl = 0; // 3;  expiration isn't supported by PRs (yet???)
    String name = "testObjectDestroyed";
    Host host = Host.getHost(0);
    final int numVMs = host.getVMCount() + 1 /* master */;
    getBlackboard().initNumInvocations();
    callbackFailure = false;
    
    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        // ignore
      }
      public void afterUpdate(EntryEvent event) {
        // ignore
      }
      public void afterDestroy(EntryEvent event) {
        getLogWriter().info("Invoking objectDestroyed listener");
        if (event.isOriginRemote()) {
          callbackAssertTrue("Wrong distributed member",
              !event.getDistributedMember().equals(getSystem().getDistributedMember()));
        } else {
          callbackAssertEquals("Wrong distributed member",
              getSystem().getDistributedMember(), event.getDistributedMember());
        }
        callbackAssertTrue("Wrong operation", event.getOperation().isDestroy());
        getBlackboard().incNumInvocations1();
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
      }
    };

    Object[] args = new Object[] { 
      name, new SubscriptionAttributes(InterestPolicy.ALL), 
      listener, new Integer(ttl), ExpirationAction.DESTROY};
    
    // Create in Master VM
    createEntry(name, ttl, ExpirationAction.DESTROY, new SubscriptionAttributes(InterestPolicy.ALL), listener);
    // create region and listeners in other VMs
    forEachVMInvoke("createSubregionWhenDestroy", args);
    
   
    // Wait for expiration
    //getLogWriter().info("Sleeping for " + ttl + " seconds");
    //Thread.sleep(ttl * 1000);

    Region region = getRegion();
    Region sub = region.getSubregion(name);
    assertNotNull(sub);
    sub.destroy(name);

    // invoked at least once
    // invoked no more than once
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return getBlackboard().getNumInvocations1() >= numVMs;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

    assertNull(newValue);
    //assertEquals(new Integer(0), oldValue);
    for (int i=0; i < host.getVMCount(); i++) {
      //Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      //assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
    assertFalse("Errors in callbacks; check logs for details", callbackFailure);
  }  

  public void testObjectAddedReplacedCACHECONTENT()
  throws CacheException, InterruptedException {

  String name = "testObjectAddedReplaced";
  getBlackboard().initNumInvocations();
  callbackFailure = false;

  GenericListener listener = new GenericListener() {
    public void afterCreate(EntryEvent event) {
      getLogWriter().info("Invoking afterCreate on listener; name=" +
                          event.getKey());
      assertEquals(event.getCallbackArgument(), event.getDistributedMember());
      assertEquals(Operation.CREATE, event.getOperation());
      callbackAssertTrue("Old value not null", event.getOldValue() == null);
      getBlackboard().incNumInvocations1();
      callbackAssertEquals("newValue is wrong", new Integer(0), 
                   event.getNewValue());
      callbackAssertNull("oldValue not null", event.getOldValue());
    }

    
    public void afterUpdate(EntryEvent event) {
      getLogWriter().info("Invoking afterUpdate on listener; name="
                          + event.getKey());
      callbackAssertEquals("Wrong callback argument",
          event.getCallbackArgument(), event.getDistributedMember());
      callbackAssertEquals("Wrong operation",
          Operation.UPDATE, event.getOperation());
      callbackAssertTrue("Old value is null", event.getOldValue() != null);
      newValue = event.getNewValue();
      oldValue = event.getOldValue();
      getBlackboard().incNumInvocations2();
      getLogWriter().info("update event new value is: " + newValue );
      getLogWriter().info("update event old value is: " + oldValue );
    } 
  };

  Object[] args = new Object[] { name, new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), listener };
  
  expectedCreateInv = 1;
  expectedUpdateInv = 0;
  
  // Create in controller VM
  LogWriter log = getLogWriter();
  log.info("TESTOBJECTADDEDREPLACED: creating entry in controller");
  createEntry(name, (SubscriptionAttributes)args[1], listener);
  log.info("TESTOBJECTADDEDREPLACED: done creating entry in controller");
  RepeatableRunnable verify = new CacheSerializableRunnable("verify") {
    public void run2() {
      assertEquals("Wrong number of 'afterCreate' listener invocations",
                   expectedCreateInv, getBlackboard().getNumInvocations1());
      assertEquals("Wrong number of 'afterUpdate' listener invocations", 
                   expectedUpdateInv, getBlackboard().getNumInvocations2()); 
    }
  };
  log.info("TESTOBJECTADDEDREPLACED: verifying");

  verify.runRepeatingIfNecessary(5000);

  log.info("TESTOBJECTADDEDREPLACED: done verifying");
  
  // Create in other VMs

  // the expected number of update events is dependent on the number of VMs.
  // Each "createEntry" does a put which fires a update event in each of
  // the other VMs that have that entry already defined at that time.
  // The first createEntry causes 0 update events.
  // The second createEntry causes 1 update events (in the first VM)
  // The third createEntry causes 2 update events, etc (in the first 2 VMs)
  Host host = Host.getHost(0);
  int vmCount = host.getVMCount();
  for (int i=0; i < vmCount; i++) {
    log.info("TESTOBJECTADDEDREPLACED: invoking update in VM " + i);
    host.getVM(i).invoke(this.getClass(), "updateEntry", args);
    log.info("TESTOBJECTADDEDREPLACED: done create in VM " + i);
    //expectedCreateInv++;
    expectedUpdateInv += 1;
    log.info("TESTOBJECTADDEDREPLACED: verifying callbacks for VM " + i);
    verify.runRepeatingIfNecessary(5000);
    log.info("TESTOBJECTADDEDREPLACED: done verifying callbacks for VM " + i);
  }
  
  // getNewValue is Integer(0)
  assertEquals("newValue is wrong", new Integer(0), newValue);
  // getOldValue is NOT AVAILABLE
  //assertEquals("oldValue is wrong", CacheEvent.NOT_AVAILABLE, oldValue);

  // test replace events
  int replaceValue = 1;
  Integer iReplaceValue = new Integer(replaceValue);

  //log.info("Number of updates before replaceEntry is " + getBlackboard().getNumInvocations2());
  replaceEntry(name, iReplaceValue);
  //log.info("Number of updates after replaceEntry is " + getBlackboard().getNumInvocations2());
  expectedUpdateInv += 1;
  verify.runRepeatingIfNecessary(5000);

  Region region = getRegion();
  Region sub = region.getSubregion(name);
  assertNotNull(sub);

  //assertEquals("oldValue is wrong", CacheEvent.NOT_AVAILABLE, getOldValue());
  assertEquals("newValue is wrong", iReplaceValue, getNewValue());
  // find the vm that should have the bucket and see if it has the correct new value
  DistributedMember bucketOwner = ((PartitionedRegion)sub).getMemberOwning(name);
  assertNotNull(bucketOwner);
  for (int i=0; i < vmCount; i++) {
    Object id = host.getVM(i).invoke(this.getClass(), "getMemberId");
    if (bucketOwner.equals(id)) {
      Object vmOldValue = host.getVM(i).invoke(getClass(), "getOldValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(replaceValue-1), vmOldValue);
      Object vmNewValue = host.getVM(i).invoke(getClass(), "getNewValue");
      assertEquals("newValue is wrong in vm " + i, iReplaceValue, vmNewValue);
    }
  }

  replaceValue++;
  iReplaceValue = new Integer(replaceValue);
  host.getVM(0).invoke(this.getClass(), "replaceEntry", new Object[] { name, iReplaceValue });
  expectedUpdateInv += 1;
  verify.runRepeatingIfNecessary(5000);

  bucketOwner = ((PartitionedRegion)sub).getMemberOwning(name);
  assertNotNull(bucketOwner);
  for (int i=0; i < vmCount; i++) {
    Object id = host.getVM(i).invoke(this.getClass(), "getMemberId");
    if (bucketOwner.equals(id)) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(replaceValue-1), vmOldValue);
      assertEquals("newValue is wrong in vm " + i, new Integer(replaceValue), vmNewValue);
    }
  }
  assertFalse("Errors in callbacks; check logs for details", callbackFailure);
}

public void testObjectInvalidatedCACHECONTENT()
  throws CacheException, InterruptedException {

  int ttl = 0;  // 5;  // seconds  expiration isn't supported in PR (yet??)
  String name = "testObjectInvalidated";
  Host host = Host.getHost(0);
  int numVMs = host.getVMCount() + 1 /* controller */;
  getBlackboard().initNumInvocations();
  callbackFailure = false;

  GenericListener listener = new GenericListener() {
    public void afterCreate(EntryEvent event) {
      // ignore
    }
    public void afterInvalidate(EntryEvent event) {
      getLogWriter().info("Invoking tests invalidated listener");
      if (event.isOriginRemote()) {
        callbackAssertTrue("Wrong event member",
            !event.getDistributedMember().equals(getSystem().getDistributedMember()));
      } else {
        callbackAssertEquals("Wrong event member",
            getSystem().getDistributedMember(), event.getDistributedMember());
      }
      callbackAssertEquals("Wrong operation",
          Operation.INVALIDATE, event.getOperation());
      getBlackboard().incNumInvocations1();
      newValue = event.getNewValue();
      oldValue = event.getOldValue();
    }
    
    public void afterUpdate(EntryEvent event) {
      // ignore
    }
  };

  Object[] args = new Object[] { 
    name, new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), 
    listener};
  
  // Create in controller VM
  createEntry(name, ttl, ExpirationAction.INVALIDATE, (SubscriptionAttributes)args[1], listener);
  // create region and listeners in other VMs
  forEachVMInvoke("createSubregion", args);
  
  // Wait for expiration
  //getLogWriter().info("Sleeping for " + ttl + " seconds");
  //Thread.sleep(ttl * 1000);
  
  // Invalidate the entry
  Region region = getRegion();
  Region sub = region.getSubregion(name);
  assertNotNull(sub);
  getLogWriter().info("About to invalidate " + name);
  sub.invalidate(name);
  

  // invoked at least once
  // invoked no more than once
  {
    int maxRetries = 100;
    while ( getBlackboard().getNumInvocations1() < numVMs ) {
      maxRetries--;
      if (maxRetries > 0) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
    assertEquals("objectInvalidated invocations wrong", 1, getBlackboard().getNumInvocations1());
  }

  assertNull(getRegion().getSubregion(name).get(name));

  DistributedMember bucketOwner = ((PartitionedRegion)sub).getMemberOwning(name);
  assertNotNull(bucketOwner);
  int vmCount = numVMs - 1;
  for (int i=0; i < vmCount; i++) {
    Object id = host.getVM(i).invoke(this.getClass(), "getMemberId");
    if (bucketOwner.equals(id)) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
  }
  assertFalse("Errors in callbacks; check logs for details", callbackFailure);
}

public void testObjectDestroyedCACHECONTENT()
  throws CacheException, InterruptedException {

  int ttl = 0; // 3;  expiration isn't supported by PRs (yet???)
  String name = "testObjectDestroyed";
  Host host = Host.getHost(0);
  int numVMs = host.getVMCount() + 1 /* master */;
  getBlackboard().initNumInvocations();
  callbackFailure = false;

  GenericListener listener = new GenericListener() {
    public void afterCreate(EntryEvent event) {
      // ignore
    }
    public void afterUpdate(EntryEvent event) {
      // ignore
    }
    public void afterDestroy(EntryEvent event) {
      getLogWriter().info("Invoking objectDestroyed listener");
      if (event.isOriginRemote()) {
        callbackAssertTrue("Wrong event member",
            !event.getDistributedMember().equals(getSystem().getDistributedMember()));
      } else {
        assertEquals("Wrong event member",
            getSystem().getDistributedMember(), event.getDistributedMember());
      }
      callbackAssertTrue("Wrong operation", event.getOperation().isDestroy());
      getBlackboard().incNumInvocations1();
      newValue = event.getNewValue();
      oldValue = event.getOldValue();
    }
  };

  Object[] args = new Object[] { 
    name, new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT), 
    listener, new Integer(ttl), ExpirationAction.DESTROY};
  
  // Create in Master VM
  createEntry(name, ttl, ExpirationAction.DESTROY, (SubscriptionAttributes)args[1], listener);
  // create region and listeners in other VMs
  forEachVMInvoke("createSubregionWhenDestroy", args);
  
  
  // Wait for expiration
  //getLogWriter().info("Sleeping for " + ttl + " seconds");
  //Thread.sleep(ttl * 1000);

  Region region = getRegion();
  Region sub = region.getSubregion(name);
  assertNotNull(sub);
  sub.destroy(name);

  // invoked at least once
  // invoked no more than once
  {
    int maxRetries = 100;
    while ( getBlackboard().getNumInvocations1() < numVMs ) {
      maxRetries--;
      if (maxRetries > 0) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
    assertEquals(1, getBlackboard().getNumInvocations1());
  }

  DistributedMember bucketOwner = ((PartitionedRegion)sub).getMemberOwning(name);
  assertNotNull(bucketOwner);
  for (int i=0; i < host.getVMCount(); i++) {
    Object id = host.getVM(i).invoke(this.getClass(), "getMemberId");
    if (bucketOwner.equals(id)) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
  }
  assertFalse("Errors in callbacks; check logs for details", callbackFailure);
}  

  
  ////////// Private test methods
  
  /**
   * Create a region and install the given listener
   * 
   * ACCESSED VIA REFLECTION
   */
  protected static void createSubregion(String name, SubscriptionAttributes subAttrs, GenericListener l)
    throws CacheException {

    Region region = getRegion();
    AttributesFactory factory =
      new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setSubscriptionAttributes(subAttrs);
    factory.addCacheListener(l);
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());

    region.createSubregion(name, factory.create());
  }
  
  /**
   * Create a region and install the given listener
   * 
   * ACCESSED VIA REFLECTION
   */
  protected static void createSubregionWhenDestroy(String name, SubscriptionAttributes subAttrs, GenericListener l, Integer ttl, ExpirationAction action)
    throws CacheException {

    Region region = getRegion();
    AttributesFactory factory =
      new AttributesFactory();
    factory.setStatisticsEnabled(true);
    factory.setSubscriptionAttributes(subAttrs);
    factory.setEntryTimeToLive(new ExpirationAttributes(ttl.intValue(), action));
    factory.addCacheListener(l);
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());

    region.createSubregion(name, factory.create());
  }
  /**
   * Create a region with one entry in this test's region with the
   * given name and attributes.
   */
  private static void createEntry(String name, int ttl, 
                                  ExpirationAction action,
                                  SubscriptionAttributes subAttrs,
                                  GenericListener l)
    throws CacheException {

    Region sub = _createRegion(name, ttl, action, subAttrs, l);
    sub.create(name, new Integer(0), sub.getCache().getDistributedSystem().getDistributedMember());
  }
  
  private static Region _createRegion(String name, int ttl,
     ExpirationAction action, SubscriptionAttributes subAttrs, GenericListener l)
    throws CacheException {

      Region region = getRegion();
      AttributesFactory factory =
        new AttributesFactory();
      factory.setStatisticsEnabled(true);
      factory.setEntryTimeToLive(new ExpirationAttributes(ttl, action));
      factory.setSubscriptionAttributes(subAttrs);
      factory.addCacheListener(l);
      factory.setPartitionAttributes((new PartitionAttributesFactory()).create());

      Region sub =
        region.createSubregion(name, factory.create());
      return sub;
    }
  
  /**
   * Create a region with one entry in this test's region with the
   * given name and attributes.
   */
  private static void createRegion(String name, int ttl, 
                                  ExpirationAction action,
                                  SubscriptionAttributes subAttrs,
                                  GenericListener l)  throws CacheException {
    _createRegion(name, ttl, action, subAttrs, l);
  }
  
  /**
   * Create an entry in this test's region with the given name
   */
  private static void createEntry(String name, SubscriptionAttributes subAttrs, GenericListener l) 
  throws CacheException {
    createEntry(name, 0, ExpirationAction.INVALIDATE, subAttrs, l);
  }
  
  /**
   * Create an entry in this test's region with the given name
   */
  private static void createRegion(String name, SubscriptionAttributes subAttrs, GenericListener l) 
  throws CacheException {
    createRegion(name, 0, ExpirationAction.INVALIDATE, subAttrs, l);
  }
  
  /**
   * Create a region and update one entry in this test's region with the
   * given name and attributes.
   */
  private static void updateEntry(String name, int ttl, 
                                  ExpirationAction action,
                                  SubscriptionAttributes subAttrs,
                                  GenericListener l)
    throws CacheException {

    Region sub = _createRegion(name, ttl, action, subAttrs, l);
    sub.put(name, new Integer(0), sub.getCache().getDistributedSystem().getDistributedMember());
  }
  
  /**
   * Update an entry in this test's region with the given name
   * 
   * ACCESSED VIA REFLECTION
   */
  protected static void updateEntry(String name, SubscriptionAttributes subAttr, GenericListener l) 
  throws CacheException {
    updateEntry(name, 0, ExpirationAction.INVALIDATE, subAttr, l);
  }
  
  /**
   * Update an entry in this test's region with the given name, assuming that the update
   * is actually causing creation of the entry
   * 
   * ACCESSED VIA REFLECTION
   */
  protected static void updateNewEntry(String name, SubscriptionAttributes subAttrs, GenericListener l) 
  throws CacheException {
    Region sub = _createRegion(name, 0, ExpirationAction.INVALIDATE, subAttrs, l);
    sub.put(name, new Integer(0), sub.getCache().getDistributedSystem().getDistributedMember());
  }
  
//  /**
//   * Create an entry in this test's region with the given name
//   */
//  private static void createEntryUsingDfltObjAttrs(String name,
//                                                   GenericListener l) 
//    throws CacheException {
//
//    createEntryUsingDfltObjAttrs(name, 0, ExpirationAction.INVALIDATE, l);
//  }
  
//  /**
//   * Create an entry in this test's region with the given name
//   */
//  private static void
//    createEntryUsingDfltObjAttrs(String name, int ttl,
//                                 ExpirationAction action,
//                                 GenericListener l) 
//    throws CacheException {
//
//    createEntry(name, ttl, action, l);
//  }

  private static void replaceEntry(String name, Object value)
    throws CacheException
  {
    Region region = getRegion();
    Region sub = region.getSubregion(name);
    assertNotNull(sub);
    sub.put(name, value, sub.getCache().getDistributedSystem().getDistributedMember());
  }

  private static Object getNewValue() {
    return newValue;
  }

//  private static Object getOldValue() {
//    return oldValue;
//  }
  
  /**
   * ACCESSED VIA REFLECTION
   */
  protected static DistributedMember getMemberId()
    throws CacheException
  {
    return getRegion().getCache().getDistributedSystem().getDistributedMember();
  }
  
  /**
   * Gets or creates a region used in this test
   */
  private static Region getRegion() 
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(REGION_NAME);;
    if (region == null) {
      AttributesFactory factory = new AttributesFactory();
      region = root.createSubregion(REGION_NAME,
                                    factory.create());
    }
    
    return region;
  }
  
  protected EventListenersBlackboard getBlackboard() {
    return EventListenersBlackboard.getInstance();
  }

  /**
   * A class that provides default implementations for the methods of
   * several listener types.
   */
  public static class GenericListener
    extends CacheListenerAdapter implements Serializable {

    ////////  CacheListener  ///////

    public void close() { }
    
    /**
     * is called when an object is newly loaded into cache.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */
    public void afterCreate(EntryEvent oevt) {
      fail("Unexpected listener callback: afterCreate");
    }
  
    /**
     * is called when an object is invalidated.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */
    public void afterInvalidate(EntryEvent oevt) {
      fail("Unexpected listener callback: afterInvalidated");
    }
  
    /**
     * is called when an object is destroyed.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */
    public void afterDestroy(EntryEvent oevt) {
      fail("Unexpected listener callback: afterDestroy");
    }
  
    /**
     * is called when an object is replaced.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */ 
    public void afterUpdate(EntryEvent oevt) {
      fail("Unexpected listener callback: afterUpdate");
    }
    
    /**
     * is called when a region is invalidated.
     * @param revt a RegionEvent to represent the source region.
     * @throws CacheException if any error occurs.
     */
    public void afterRegionInvalidate(RegionEvent revt) {
      fail("Unexpected listener callback: afterRegionInvalidate");
    }
  
    /**
     * is called when a region is destroyed.
     * @param revt a RegionEvent to represent the source region.
     * @throws CacheException if any error occurs.
     */
    public void afterRegionDestroy(RegionEvent revt) {
//       fail("Unexpected listener callback: afterRegionDestroy");
    }    
  }
  
}
