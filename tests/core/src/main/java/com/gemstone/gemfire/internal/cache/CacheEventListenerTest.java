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
import dunit.*;

import java.io.Serializable;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.*;

/**
 * This class tests event triggering and handling in distributed
 * regions.  Even though it exercises some of the same functionality
 * as {@link com.gemstone.gemfire.cache30.MultiVMRegionTestCase}, this
 * test is still interesting because of its use of all DUnit VMs and
 * the Hydra {@link Blackboard blackboard}.
 *
 * @author Dave Monnie
 * @author David Whitlock (ported to 3.0)
 *
 * @since 2.0
 */
public class CacheEventListenerTest extends DistributedCacheTestCase {
  
  static final String REGION_NAME = "CacheEventListenerTest";
  static final Scope SCOPE = Scope.DISTRIBUTED_ACK;
  
  volatile static Object newValue, oldValue;
  
  volatile int expectedCreateInv;
  volatile int expectedUpdateInv;
    
  public CacheEventListenerTest(String name) {
    super(name);
  }

  /////////  Public test methods

  public void testObjectAddedReplaced()
    throws CacheException, InterruptedException {

    String name = "testObjectAddedReplaced";
//    int numSystems = Host.getHost(0).getSystemCount();
    int numVMs = Host.getHost(0).getVMCount() + 1 /* controller */;
    getBlackboard().initNumInvocations();

    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        getLogWriter().info("Invoking afterCreate on listener; name=" +
                            event.getKey());
        assertEquals(event.getCallbackArgument(), event.getDistributedMember());
        assertEquals(Operation.CREATE, event.getOperation());
        Assert.assertTrue(event.getOldValue() == null);
        getBlackboard().incNumInvocations1();
        assertEquals("newValue is wrong", new Integer(0), 
                     event.getNewValue());
        assertNull("oldValue not null", event.getOldValue());
        if (event.getSerializedOldValue() != null) {
          assertEquals(event.getOldValue(), event.getSerializedOldValue().getDeserializedValue());
        }
        if (event.getSerializedNewValue() != null) {
          assertEquals(event.getNewValue(), event.getSerializedNewValue().getDeserializedValue());
        }
        getLogWriter().info("create event new value is: " + event.getNewValue());
      }

      
      public void afterUpdate(EntryEvent event) {
        getLogWriter().info("Invoking afterUpdate on listener; name="
                            + event.getKey());
        assertEquals(event.getCallbackArgument(), event.getDistributedMember());
        assertEquals(Operation.UPDATE, event.getOperation());
        Assert.assertTrue(event.getOldValue() != null);
        if (event.getSerializedOldValue() != null) {
          assertEquals(event.getOldValue(), event.getSerializedOldValue().getDeserializedValue());
        }
        if (event.getSerializedNewValue() != null) {
          assertEquals(event.getNewValue(), event.getSerializedNewValue().getDeserializedValue());
        }
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
        getBlackboard().incNumInvocations2();
        getLogWriter().info("update event new value is: " + newValue );
        getLogWriter().info("update event old value is: " + oldValue );
      } 
    };

    Object[] args = new Object[] { name, listener };
    
    expectedCreateInv = 1;
    expectedUpdateInv = 0;
    
    // Create in controller VM
    LogWriter log = getLogWriter();
    log.info("TESTOBJECTADDEDREPLACED: creating entry in controller");
    createEntry(name, listener);
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
      log.info("TESTOBJECTADDEDREPLACED: invoking create in VM " + i);
      host.getVM(i).invoke(this.getClass(), "createEntry", args);
      log.info("TESTOBJECTADDEDREPLACED: done create in VM " + i);
      expectedCreateInv++;
      expectedUpdateInv += (i + 1);
      log.info("TESTOBJECTADDEDREPLACED: verifying callbacks for VM " + i);
      verify.runRepeatingIfNecessary(5000);
      log.info("TESTOBJECTADDEDREPLACED: done verifying callbacks for VM " + i);
    }
    
    // getNewValue is Integer(0)
    assertEquals("newValue is wrong", new Integer(0), newValue);
    // getOldValue is Integer(0)
    assertEquals("oldValue is wrong", new Integer(0), oldValue);

    // test replace events
    int replaceValue = 1;

    replaceEntry(name, new Integer (replaceValue));
    expectedUpdateInv += numVMs;
    verify.runRepeatingIfNecessary(5000);

    assertEquals("oldValue is wrong", new Integer(replaceValue-1), getOldValue());
    assertEquals("newValue is wrong", new Integer(replaceValue), getNewValue());
    for (int i=0; i < vmCount; i++) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(replaceValue-1), vmOldValue);
      assertEquals("newValue is wrong in vm " + i, new Integer(replaceValue), vmNewValue);
    }

    replaceValue++;
    host.getVM(0).invoke(this.getClass(), "replaceEntry", new Object[] { name, new Integer(replaceValue)});
    expectedUpdateInv += numVMs;
    verify.runRepeatingIfNecessary(5000);

    assertEquals("oldValue is wrong", new Integer(replaceValue-1), getOldValue());
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
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(replaceValue-1), vmOldValue);
      assertEquals("newValue is wrong in vm " + i, new Integer(replaceValue), vmNewValue);
    }
  }

  public void testObjectInvalidated()
    throws CacheException, InterruptedException {

    int ttl = 5;  // seconds
    String name = "testObjectInvalidated";
    Host host = Host.getHost(0);
//    int numSystems = host.getSystemCount();
    final int numVMs = host.getVMCount() + 1 /* controller */;
    getBlackboard().initNumInvocations();
    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        // ignore
      }
      public void afterInvalidate(EntryEvent event) {
        getLogWriter().info("Invoking tests invalidated listener");
        if (event.isOriginRemote()) {
          assertTrue(!event.getDistributedMember().equals(getSystem().getDistributedMember()));
        } else {
          assertEquals(getSystem().getDistributedMember(), event.getDistributedMember());
        }
        assertEquals(Operation.EXPIRE_INVALIDATE, event.getOperation());
        getBlackboard().incNumInvocations1();
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
      }
      
      public void afterUpdate(EntryEvent event) {
        // ignore
      }
    };

    Object[] args = new Object[] { 
      name, 
      new Integer(ttl), 
      ExpirationAction.INVALIDATE, 
      listener};
    
    // Create in controller VM
    createEntry(name, ttl, ExpirationAction.INVALIDATE, listener);
    // Create in other VMs
    long start = System.currentTimeMillis();
    forEachVMInvoke("createEntry", args);
    long time = System.currentTimeMillis() - start;
    assertEquals("Test timing intolerance: entry creation took " + time +
      "ms. Increase expiration time in test", 0, getBlackboard().getNumInvocations1());
    
    // Wait for invalidation
    getLogWriter().info("Sleeping for " + ttl + " seconds");
    Thread.sleep(ttl * 1000);

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
    assertEquals(new Integer(0), oldValue);

    for (int i=0; i < host.getVMCount(); i++) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
  }

  /*
   * @todo davidw Re-enabled when distributed destruction is
   * implemented. 
   */
  public void testObjectDestroyed()
    throws CacheException, InterruptedException {

    int ttl = 3;  // seconds
    String name = "testObjectDestroyed";
    Host host = Host.getHost(0);
//    int numSystems = host.getSystemCount();
    final int numVMs = host.getVMCount() + 1 /* master */;
    getBlackboard().initNumInvocations();
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
          assertTrue(!event.getDistributedMember().equals(getSystem().getDistributedMember()));
        } else {
          assertEquals(getSystem().getDistributedMember(), event.getDistributedMember());
        }
        assertEquals(Operation.EXPIRE_DESTROY, event.getOperation());
        getBlackboard().incNumInvocations1();
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
      }
    };

    Object[] args = new Object[] { 
      name, 
      new Integer(ttl), 
      ExpirationAction.DESTROY, 
      listener};
    
    // Create in Master VM
    createEntry(name, ttl, ExpirationAction.DESTROY, listener);
    // Create in other VMs
    forEachVMInvoke("createEntry", args);
    
    // Wait for invalidation
    getLogWriter().info("Sleeping for " + ttl + " seconds");
    Thread.sleep(ttl * 1000);

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
    assertEquals(new Integer(0), oldValue);
    for (int i=0; i < host.getVMCount(); i++) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
  }  

  public void testInvalidatedViaExpiration()
    throws CacheException, InterruptedException {

    int ttl = 6;  // seconds
    final String name = "testUnloadedViaExpiration";
    Host host = Host.getHost(0);
//    int numSystems = host.getSystemCount();
    final int numVMs = host.getVMCount() + 1; // master 
    getBlackboard().initNumInvocations();
    GenericListener listener = new GenericListener() {
      public void afterCreate(EntryEvent event) {
        // ignore
      }
      public void afterUpdate(EntryEvent event) {
        // ignore
      }

      public void afterInvalidate(EntryEvent event) {
        Object key = event.getKey();
        boolean isPresentLocally = 
          event.getRegion().getEntry(key).getValue() != null;

        String s = "Invoking test's invalidate listener " +
          "(via expiration)" + " isPresentLocally=" + 
          isPresentLocally + "; newValue=" + event.getNewValue();
        getLogWriter().info(s);

        assertTrue(!event.isOriginRemote());
        assertEquals(getSystem().getDistributedMember(), event.getDistributedMember());
        assertEquals(Operation.EXPIRE_LOCAL_INVALIDATE, event.getOperation());
        getBlackboard().incNumInvocations1();
        newValue = event.getNewValue();
        oldValue = event.getOldValue();
      }
    };

    Object[] args = new Object[] { 
      name, 
      new Integer(ttl), 
      ExpirationAction.LOCAL_INVALIDATE,
      listener};
    
    // Create in controller VM
    createEntry(name, ttl, ExpirationAction.LOCAL_INVALIDATE, listener);
    // Create in other VMs
    forEachVMInvoke("createEntry", args);

    // Wait for invalidation
    getLogWriter().info("Sleeping for " + ttl + " seconds");
    Thread.sleep(ttl * 1000);

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
    assertEquals(numVMs, getBlackboard().getNumInvocations1());

    // not present locally
    Region region = getRegion().getSubregion(name);
    assertNotNull(region);
    Region.Entry entry = region.getEntry(name);
    assertNotNull(entry);
    assertNull("is present locally", entry.getValue());

    assertEquals(new Integer(0), oldValue);
    assertNull(newValue);
    for (int i=0; i < host.getVMCount(); i++) {
      Object vmOldValue = host.getVM(i).invoke(this.getClass(), "getOldValue");
      Object vmNewValue = host.getVM(i).invoke(this.getClass(), "getNewValue");
      assertEquals("oldValue is wrong in vm " + i, new Integer(0), vmOldValue);
      assertNull("newValue is wrong in vm " + i, vmNewValue);
    }
  }

  
  ////////// Private test methods
  
  /**
   * Create a region with one entry in this test's region with the
   * given name and attributes.
   */
  private static void createEntry(String name, int ttl, 
                                  ExpirationAction action,
                                  GenericListener l)
    throws CacheException {

    Region region = getRegion();
    AttributesFactory factory =
      new AttributesFactory(region.getAttributes());
    factory.setStatisticsEnabled(true);
    factory.setEntryTimeToLive(new ExpirationAttributes(ttl, action));
    factory.setScope(SCOPE);
    factory.setCacheListener(l);

    Region sub =
      region.createSubregion(name, factory.create());
    sub.create(name, new Integer(0), sub.getCache().getDistributedSystem().getDistributedMember());
  }
  
  /**
   * Create an entry in this test's region with the given name
   */
  private static void createEntry(String name, GenericListener l) 
  throws CacheException {
    createEntry(name, 0, ExpirationAction.INVALIDATE, l);
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

  private static Object getOldValue() {
    return oldValue;
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
      factory.setScope(SCOPE);
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
