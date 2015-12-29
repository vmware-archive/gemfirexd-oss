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
/*
 * 
 * Created on Apr 3, 2006
 * @since 5.0
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.Alert;
import com.gemstone.gemfire.admin.AlertListener;
import com.gemstone.gemfire.admin.SystemMemberCacheEvent;
import com.gemstone.gemfire.admin.SystemMemberCacheListener;
import com.gemstone.gemfire.admin.SystemMemberRegionEvent;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.PureJavaMode;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * Test PartitionedRegion system administration events
 * 
 * @since 5.0
 */
public class PartitionedRegionSystemMemberRegionListenerDUnitTest extends AdminDUnitTestCase
{

  public static AdminDistributedSystem ads = null;

  public PartitionedRegionSystemMemberRegionListenerDUnitTest(String name) {
    super(name);
  }

  public static DistributedMember getVMDM() {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    assertNotNull(ds);
    return ds.getDistributedMember();
  }
  
  /**
   * This test performs following operations <br>
   * 1. create admin node and add SystemMemberCacheListener to it</br> <br>
   * 2. create partition region on other node</br> <br>
   * 3.destroy created partition region.</br> <br>
   * 4. verify that SystemMemberCacheListener invokes</br>
   * 
   * @throws Exception
   */
  public void testAdminAPIInPartitionedRegions() throws Exception  {
    Host host = Host.getHost(0);
    VM cacheVm1 = host.getVM(1);
    VM cacheVm2 = host.getVM(2);
    TestAlertListener tal = TestAlertListener.getInstance();

    tal.resetList();

    assertFalse(isConnectedToDS());
    AdminDistributedSystemImpl adsi = (AdminDistributedSystemImpl) getAdminDistributedSystem();
    try {
      adsi.addCacheListener(TestSystemMemberCacheListener.getInstance());
      adsi.addAlertListener(tal); 
    } catch (Exception e) {
      getLogWriter().severe("Exception", e);
      fail("Exception e: " + e);
    }

    // Make sure that the admin alert listener is recognized in all other 
    // members before proceeding
    final DistributedMember adminMemberId = adsi.getDistributedMember();
    CacheSerializableRunnable verifyRegistration = new CacheSerializableRunnable("Verify admin listener for " + getName()) {
      public void run2() throws CacheException
      {
        final InternalDistributedSystem ds = getSystem();
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return ds.hasAlertListenerFor(adminMemberId);
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
      }
    };
    cacheVm1.invoke(verifyRegistration);
    cacheVm2.invoke(verifyRegistration);

    final int redundancy = 0;
    final int localMaxMemory = 200;
    final String prPrefix = getUniqueName();

    // creating partition region
    CacheSerializableRunnable createRegion = new CacheSerializableRunnable("testCreateCacheVM") {

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        try {
          cache.createRegion(
              prPrefix, createRegionAttrsForPR(redundancy, localMaxMemory));
        }
        catch (RegionExistsException ex) {
          fail("Got incorrect exception because the partition region being created prior to local region");
        }
      }
    };
    cacheVm1.invoke(createRegion);
    final DistributedMember vm1dm = (DistributedMember) cacheVm1.invoke(PartitionedRegionSystemMemberRegionListenerDUnitTest.class, "getVMDM");

    final int vm1pid = cacheVm1.getPid();
    {
      TestSystemMemberCacheListener rel = TestSystemMemberCacheListener.getInstance();
      rel.waitForCreate(prPrefix, vm1dm);
      synchronized(rel) {
        HashSet hs = (HashSet) rel.createSet.get(Region.SEPARATOR + prPrefix);
        assertNotNull(hs);
        if(!PureJavaMode.isPure()) {
          String s = hs.toString();
          assertTrue("vm1pid " + vm1pid + " not found in createSet: " + s, s.indexOf(Integer.toString(vm1pid)) > -1);
        }
      }
    }

    cacheVm2.invoke(createRegion);
    final DistributedMember vm2dm = (DistributedMember) cacheVm2.invoke(PartitionedRegionSystemMemberRegionListenerDUnitTest.class, "getVMDM");
    {
      TestSystemMemberCacheListener rel = TestSystemMemberCacheListener.getInstance();
      rel.waitForCreate(prPrefix, vm2dm);
    }

    CacheSerializableRunnable destroyRegion = new CacheSerializableRunnable("destroyRegion") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region pr = cache.getRegion(Region.SEPARATOR + prPrefix);
        assertNotNull("This region is null", pr);
        pr.destroyRegion();
      }
    };
    cacheVm2.invoke(destroyRegion);

    {
      TestSystemMemberCacheListener rel = TestSystemMemberCacheListener.getInstance();
      rel.waitForDestroy(prPrefix, vm2dm);
    }

    {
      TestSystemMemberCacheListener rel = TestSystemMemberCacheListener.getInstance();
      rel.waitForDestroy(prPrefix, vm1dm);
      rel.waitForDestroy(prPrefix, vm2dm);

      LogWriter lw = getLogWriter();
      synchronized(tal) {
        int j = 0;
        for (Iterator i = tal.alerts.iterator(); i.hasNext(); ) {
          Alert a = (Alert) i.next();
          lw.severe("Unexpected Alert " + j + ":" + a);
        }
        assertEquals("Alerts were found", 0, tal.alerts.size());
      }
    }
  }

  /**
   * Ensure that an Alert message is sent when the local_max_memory is exceeded.
   */
  public void testAlertWhenLocalMaxExceeded() throws Exception
  {
    Host host = Host.getHost(0);
    VM cacheVm1 = host.getVM(1);
    VM cacheVm2 = host.getVM(2);
    TestAlertListener tal = TestAlertListener.getInstance();

    tal.resetList();
    assertFalse(isConnectedToDS());
    // add AlertListener - note: msg to other members is asynchronous!!
    AdminDistributedSystemImpl adsi = (AdminDistributedSystemImpl) getAdminDistributedSystem(); 
    try {
      adsi.addAlertListener(tal); 
    } catch (Exception e) {
      getLogWriter().severe("Exception", e);
      fail("Exception e: " + e);
    }
    
    // Make sure that the admin alert listener is recognized in all other 
    // members before proceeding
    final DistributedMember adminMemberId = adsi.getDistributedMember();
    CacheSerializableRunnable verifyRegistration = new CacheSerializableRunnable("Verify admin listener") {
      public void run2() throws CacheException
      {
        final InternalDistributedSystem ds = getSystem();
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return ds.hasAlertListenerFor(adminMemberId);
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
      }
    };
    cacheVm1.invoke(verifyRegistration);
    cacheVm2.invoke(verifyRegistration);
    
    final int redundancy = 0;
    final int localMaxMemory = 2;
    final String prPrefix = getUniqueName();
    final int numBuckets = 5;
    final int valBytes = 1024*1200;
    
    // creating partition region
    CacheSerializableRunnable createRegion = new CacheSerializableRunnable("testCreateCacheVM") {
      public void run2() throws CacheException
      {
        getCache(); // create a cache
        Region pr = new RegionFactory()
          .setPartitionAttributes(
              new PartitionAttributesFactory()
              .setRedundantCopies(redundancy)
              .setLocalMaxMemory(localMaxMemory)
              .setTotalNumBuckets(numBuckets)
              .create())
          .create(prPrefix);
        int start = pr.size();
        int end = start + localMaxMemory;
        getLogWriter().info("start: " + start + " end: " + end);
        for (int i=start; i<end; i++) {
          pr.put(new Integer(i), "createBucket");  // create bucket first to avoid denial due to large value .. 1 Mb
          pr.put(new Integer(i), new byte[valBytes]); // Then fill it          
        }
      }
    };
    
    // invoke PR creation in VMs 1 and 2 so two alerts will be generated
    
    cacheVm1.invoke(createRegion);
    final DistributedMember vm1dm = (DistributedMember) cacheVm1.invoke(PartitionedRegionSystemMemberRegionListenerDUnitTest.class, "getVMDM");
    cacheVm2.invoke(createRegion);
    final DistributedMember vm2dm = (DistributedMember) cacheVm2.invoke(PartitionedRegionSystemMemberRegionListenerDUnitTest.class, "getVMDM");
    
    HashSet hs = new HashSet();
    hs.add(vm1dm);
    hs.add(vm2dm);

    // wait for the two alerts to arive in this member - might fail if
    //   PRs are created before the two members process the addAlertListener
    //   this needs to be fixed or test will intermittently fail
    
    tal.waitForEvents(2);
    // Verify that the events received are the correct ones (correct message)
    synchronized(tal) {
      int numExceedAlerts = 0;
      for (Iterator i = tal.alerts.iterator(); i.hasNext();) {
        Alert a = (Alert) i.next();
        if (a.getMessage().indexOf(PartitionedRegionDataStore.EXCEEDED_LOCAL_MAX_MSG) > 0) {
          assertTrue(hs.contains(a.getSystemMember().getDistributedMember()));
          numExceedAlerts++;
        }
      }
      assertEquals(2, numExceedAlerts);
    }
  }
  
  /**
   * Ensure that an Alert message is not sent when the local_max_memory is exceeded if eviction is enabled,
   */
  public void testNoAlertWhenLocalMaxExceededWithEviction() throws Exception
  {
    Host host = Host.getHost(0);
    VM cacheVm1 = host.getVM(1);
    VM cacheVm2 = host.getVM(2);
    TestAlertListener tal = TestAlertListener.getInstance();

    tal.resetList();
    assertFalse(isConnectedToDS());
    // add AlertListener - note: msg to other members is asynchronous!!
    AdminDistributedSystemImpl adsi = (AdminDistributedSystemImpl) getAdminDistributedSystem(); 
    try {
      adsi.addAlertListener(tal); 
    } catch (Exception e) {
      getLogWriter().severe("Exception", e);
      fail("Exception e: " + e);
    }
    
    // Make sure that the admin alert listener is recognized in all other 
    // members before proceeding
    final DistributedMember adminMemberId = adsi.getDistributedMember();
    CacheSerializableRunnable verifyRegistration = new CacheSerializableRunnable("Verify admin listener") {
      public void run2() throws CacheException
      {
        final InternalDistributedSystem ds = getSystem();
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return ds.hasAlertListenerFor(adminMemberId);
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
      }
    };
    cacheVm1.invoke(verifyRegistration);
    cacheVm2.invoke(verifyRegistration);
    
    final int redundancy = 0;
    final int localMaxMemory = 2;
    final String prPrefix = getUniqueName();
    final int numBuckets = 5;
    final int valBytes = 1024*1200;
    
    // creating partition region
    CacheSerializableRunnable createRegion = new CacheSerializableRunnable("testCreateCacheVM") {
      public void run2() throws CacheException
      {
        getCache(); // create a cache
        Region pr = new RegionFactory()
          .setPartitionAttributes(
              new PartitionAttributesFactory()
              .setRedundantCopies(redundancy)
              .setLocalMaxMemory(localMaxMemory)
              .setTotalNumBuckets(numBuckets)
              .create())
              .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(Integer.MAX_VALUE,
                EvictionAction.LOCAL_DESTROY))
          .create(prPrefix);
        int start = pr.size();
        int end = start + localMaxMemory;
        getLogWriter().info("start: " + start + " end: " + end);
        for (int i=start; i<end; i++) {
          pr.put(new Integer(i), "createBucket");  // create bucket first to avoid denial due to large value .. 1 Mb
          pr.put(new Integer(i), new byte[valBytes]); // Then fill it          
        }
      }
    };
    
    // invoke PR creation in VMs 1 and 2 so two alerts will be generated
    
    cacheVm1.invoke(createRegion);
    final DistributedMember vm1dm = (DistributedMember) cacheVm1.invoke(PartitionedRegionSystemMemberRegionListenerDUnitTest.class, "getVMDM");
    cacheVm2.invoke(createRegion);
    final DistributedMember vm2dm = (DistributedMember) cacheVm2.invoke(PartitionedRegionSystemMemberRegionListenerDUnitTest.class, "getVMDM");
    
    //wait for alerts to propegate, if they were generated.
    pause(500);
    
    //make sure there were no alerts
    synchronized(tal) {
      assertEquals(0, tal.alerts.size());
    }
  }

  protected RegionAttributes createRegionAttrsForPR(int red, int localMaxMem)
  {
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(red)
        .setLocalMaxMemory(localMaxMem).create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }

  static class TestSystemMemberCacheListener implements
      SystemMemberCacheListener
  {
    
    public static TestSystemMemberCacheListener instance;
    
    protected final HashMap createSet = new HashMap();
    private final HashMap destroySet = new HashMap();
    
    private boolean seenEvent = false; 
    
    public synchronized static TestSystemMemberCacheListener getInstance() {
      if (instance == null) {
        instance = new TestSystemMemberCacheListener();
      }
      return instance;
    }
    
    private TestSystemMemberCacheListener() {
    }

    public synchronized void afterRegionCreate(SystemMemberRegionEvent event)
    {
      HashSet hosts = (HashSet) this.createSet.get(event.getRegionPath());
      if (hosts != null) {
        hosts.add(event.getDistributedMember());
      } else {
        HashSet s = new HashSet();
        s.add(event.getDistributedMember());
        this.createSet.put(event.getRegionPath(), s);
      }
      getLogWriter().info("SystemMemberCacheListener::regionCreated event, Region path is "
              + event.getRegionPath());
      this.seenEvent = true;
      notifyAll();
    }

    public synchronized void afterRegionLoss(SystemMemberRegionEvent event)
    {
      HashSet hosts = (HashSet) this.destroySet.get(event.getRegionPath());
      if (hosts != null) {
        hosts.add(event.getDistributedMember());
      } else {
        HashSet s = new HashSet();
        s.add(event.getDistributedMember());
        this.destroySet.put(event.getRegionPath(), s);
      }
      getLogWriter().info("SystemMemberCacheListener::regionDestroyed event, Region path is "
              + event.getRegionPath());
      this.seenEvent = true;
      notifyAll();
    }
    public void afterCacheCreate(SystemMemberCacheEvent event) {
    }
    public void afterCacheClose(SystemMemberCacheEvent event) {
    }
    
    public void waitForCreate(String rName, DistributedMember fromMember) throws InterruptedException {
      waitForEvent(rName, fromMember, this.createSet);
    }
    public void waitForDestroy(String rName, DistributedMember fromMember) throws InterruptedException {
      waitForEvent(rName, fromMember, this.destroySet);
    }
    private synchronized void waitForEvent(String rName, DistributedMember fromMember, HashMap eventMap) throws InterruptedException {
      HashSet hosts;
      final int waitTime = 250;
      final long maxWaitFail = 60000;
      final long startTime = System.currentTimeMillis();
      do {
        hosts = (HashSet) eventMap.get('/' + rName);
        if (hosts != null && hosts.contains(fromMember)) {
          return;
        }
        while (! this.seenEvent) {
          if ((System.currentTimeMillis() - startTime) > maxWaitFail) {
            fail("Waited over " + maxWaitFail + " ms");
          }
          wait(waitTime);
        }
        this.seenEvent = false;
      } while (true);
    }
  }

  static class TestAlertListener implements AlertListener
  {
    // accesses to the alerts field must be synchronized on this instance
    protected ArrayList alerts;
    
    private static TestAlertListener instance;
    
    public synchronized static TestAlertListener getInstance() {
      if (instance == null) {
        instance = new TestAlertListener();
        instance.resetList();
      }
      return instance;
    }
    public TestAlertListener() {
    }

    public synchronized void resetList() {
      alerts = new ArrayList();
    }
    
    public synchronized void alert(Alert alert)
    {
      this.alerts.add(alert);
      notifyAll();
    }
    
    public synchronized int getAlertCount() {
      return this.alerts.size();
    }
    
    public synchronized Alert[] getAlerts() {
      Alert[] ret = new Alert[this.alerts.size()];
      this.alerts.toArray(ret);
      return ret;
    }
    
    /**
     * Wait for some number of events
     * @param totalEvents
     * @throws InterruptedException
     */
    public synchronized void waitForEvents(int totalEvents) throws InterruptedException {
   
      final long startTime = System.currentTimeMillis();
      do {
        if (this.alerts.size() >= totalEvents) {
          return;
        }
        waitForAnEvent(startTime);
      } while (true);
    }

    /**
     * Wait for some number of events seen matching the specified severity
     * @param numToWaitFor
     * @param severity
     * @throws InterruptedException
     */
    public synchronized void waitForEventsWithSeverity(int numToWaitFor, int severity) throws InterruptedException {
      final long startTime = System.currentTimeMillis();
      do {
        int numSeenWithSeverity = 0;
        for (Iterator i= this.alerts.iterator(); i.hasNext(); ) {
          Alert a = (Alert) i.next();
          if (a.getLevel().getSeverity() == severity) {
            numSeenWithSeverity++;
          }
        }
        if (numSeenWithSeverity >= numToWaitFor) {
          return;
        }
        waitForAnEvent(startTime);

      } while (true);
    }
    
    /**
     * Must be called with synchronization held
     * @param startTime
     * @throws InterruptedException
     */
    private void waitForAnEvent(final long startTime) throws InterruptedException {
      assertTrue(Thread.holdsLock(this));
      final int waitTime = 200;
      final long maxWaitFail = 3 * 60 * 1000;
      int lastSize;
      ArrayList lastAlertList;
      // Keep waiting while the list has not changed (both the list reference and 
      // its size)
      do {
        lastAlertList = this.alerts;
        lastSize = lastAlertList.size();
        wait(waitTime);
        if ((System.currentTimeMillis() - startTime) > maxWaitFail) {
          fail("Waited over " + maxWaitFail + " ms");
        }
        if (lastAlertList != this.alerts) {
          fail("TestAlertListener was reset");
        }
      } while (lastSize == lastAlertList.size());
    }


  }
}
