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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalRole;
import dunit.*;

import java.io.*;
import java.util.*;

/**
 * Tests the functionality of the {@link SystemMemberRegion}
 * administration API.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class SystemMemberRegionDUnitTest extends AdminDUnitTestCase {

  /**
   * Creates a new <code>SystemMemberRegionDUnitTest</code>
   */
  public SystemMemberRegionDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  public static void caseSetUp() {
    // this makes sure we don't have any connection left over from previous tests
    disconnectAllFromDS();
  }
  
  /**
   * Tests that region attributes are accessed correctly with the
   * admin API.
   */
  public void testRegionAttributes() throws Exception {
    invokeInEveryVM(DistributedTestCase.class,
        "disconnectFromDS");
    
    VM vm = Host.getHost(0).getVM(0);

    final String name = this.getUniqueName() + "_root";
    final int concurrencyLevel = 25;
    final int initialCapacity = 200;
    final float loadFactor = 0.86f;
    final boolean statisticsEnabled = true;

    final File diskDir =
      new File(this.getUniqueName()).getCanonicalFile();
    diskDir.mkdirs();
    //          Set properties for Asynch writes
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setBytesThreshold(456L);
    dwaf.setTimeInterval(123L);
    
    final DiskWriteAttributes dwa = dwaf.create();

    final ExpirationAttributes entryIdle =
      new ExpirationAttributes(76, ExpirationAction.INVALIDATE);
    final ExpirationAttributes entryTtl =
      new ExpirationAttributes(82, ExpirationAction.DESTROY);
    final DataPolicy dataPolicy = DataPolicy.REPLICATE;
    final ExpirationAttributes regionIdle =
      new ExpirationAttributes(77, ExpirationAction.DESTROY);
    final ExpirationAttributes regionTtl =
      new ExpirationAttributes(92, ExpirationAction.INVALIDATE);

    final Scope scope = Scope.DISTRIBUTED_ACK;
    final Object userAttribute = new Long(241234);
    
    final AdminDistributedSystem tc = this.tcSystem;
    vm.invoke(new CacheSerializableRunnable("Create cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setCacheListener(new CacheListener2());
          factory.setCacheLoader(new CacheLoader2());
          factory.setCacheWriter(new CacheWriter2());
          factory.setConcurrencyLevel(concurrencyLevel);
          factory.setDiskDirs(new File[] { diskDir });
          factory.setDiskWriteAttributes(dwa);
          factory.setStatisticsEnabled(statisticsEnabled);
          factory.setEntryIdleTimeout(entryIdle);
          factory.setEntryTimeToLive(entryTtl);
          factory.setInitialCapacity(initialCapacity);
          factory.setKeyConstraint(String.class);
          factory.setLoadFactor(loadFactor);
          factory.setDataPolicy(dataPolicy);
          factory.setRegionIdleTimeout(regionIdle);
          factory.setRegionTimeToLive(regionTtl);
          factory.setScope(scope);
          factory.setEarlyAck(false);
          MembershipAttributes membershipAttributes = 
            new MembershipAttributes(new String[] {"A"}, 
                                      LossAction.LIMITED_ACCESS, 
                                      ResumptionAction.NONE);
          factory.setMembershipAttributes(membershipAttributes);

          RegionAttributes attrs = factory.create();
          Region region = cache.createRegion(name, attrs);
          region.setUserAttribute(userAttribute);
        }
      });

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return tc.getSystemMemberApplications().length == 1;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return "system member applications length never became exactly 1";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);

    SystemMember[] apps = tc.getSystemMemberApplications();
    if (apps.length != 1) {
      StringBuffer sb = new StringBuffer("Applications are ");
      for (int i = 0; i < apps.length; i++) {
        sb.append(apps[i]);
        sb.append(" ");
      }
      assertEquals(sb.toString(), 1, apps.length);
    }

    SystemMemberCache cache = waitForCache(vm);
    cache.refresh();

    Set names = cache.getRootRegionNames();
    assertEquals(1, names.size());
    assertEquals(name, (String) names.iterator().next());
    
    SystemMemberRegion root = cache.getRegion(name);
    assertNotNull(root);

    String expected = CacheListener2.class.getName();
    String actual = root.getCacheListener();
    if (!expected.equals(actual)) {
      String s = "Expected <" + expected + "> got <" + actual + ">";
      fail(s);
    }
    assertEquals(CacheLoader2.class.getName(),
                 root.getCacheLoader());
    assertEquals(CacheWriter2.class.getName(),
                 root.getCacheWriter());
    assertNotNull(root.getEvictionAttributes());
    assertNotNull(root.getEvictionAttributes().getAction());
    assertTrue(root.getEvictionAttributes().getAction().isNone());
    assertTrue(root.getEvictionAttributes().getAlgorithm().isNone());
    assertEquals(concurrencyLevel, root.getConcurrencyLevel());
    File[] diskDirs = root.getDiskDirs();
    assertEquals(1, diskDirs.length);
    assertEquals(diskDir, diskDirs[0]);
    assertTrue(root.getStatisticsEnabled());
    assertEquals(entryIdle.getAction(),
                 root.getEntryIdleTimeoutAction());
    assertEquals(entryIdle.getTimeout(),
                 root.getEntryIdleTimeoutTimeLimit());
    assertEquals(entryTtl.getAction(),
                 root.getEntryTimeToLiveAction());
    assertEquals(entryTtl.getTimeout(),
                 root.getEntryTimeToLiveTimeLimit());
    assertEquals(initialCapacity, root.getInitialCapacity());
    assertEquals(String.class.getName(), root.getKeyConstraint());
    assertEquals(loadFactor, root.getLoadFactor(), 0.0f);
    assertEquals(dataPolicy, root.getDataPolicy());
    assertEquals(regionIdle.getAction(),
                 root.getRegionIdleTimeoutAction());
    assertEquals(regionIdle.getTimeout(),
                 root.getRegionIdleTimeoutTimeLimit());
    assertEquals(regionTtl.getAction(),
                 root.getRegionTimeToLiveAction());
    assertEquals(regionTtl.getTimeout(),
                 root.getRegionTimeToLiveTimeLimit());
    assertEquals(scope, root.getScope());
    assertTrue(!root.getEarlyAck());

    String attrDesc = root.getUserAttribute();
    assertTrue(attrDesc.indexOf(userAttribute.toString()) != -1);
    
    // test membership attributes
    assertNotNull(root.getMembershipAttributes());
    assertTrue(root.getMembershipAttributes().getRequiredRoles().size() == 1);
    assertTrue(root.getMembershipAttributes().getRequiredRoles().contains(
      InternalRole.getRole("A")));
    assertNotNull(root.getMembershipAttributes().getLossAction());
    assertTrue(root.getMembershipAttributes().getLossAction().isLimitedAccess());
    assertNotNull(root.getMembershipAttributes().getResumptionAction());
    assertTrue(root.getMembershipAttributes().getResumptionAction().isNone());
  }

  static public DistributedMember getMyName() {
    Cache cache2;
    try {
      cache2 = CacheFactory.getAnyInstance();
    }
    catch (CancelException e) {
      return null;
    }
    DistributedMember result = cache2.getDistributedSystem().getDistributedMember();
    return result;
  }
  
  private SystemMemberCache waitForCache(VM vm) throws Exception {
    // First, find the SystemMember that corresponds to this VM
    long deadLine = System.currentTimeMillis() + 60 * 1000;
    for (;;) {
      DistributedMember vmdm = (DistributedMember)vm.invoke(SystemMemberRegionDUnitTest.class, "getMyName");
      if (vmdm != null) {
        SystemMember allMembers[] = this.tcSystem.getSystemMemberApplications();
        SystemMember s;
        for (int i = 0; i < allMembers.length; i ++) {
          s = allMembers[i];
          DistributedMember sdm = s.getDistributedMember();
          assertTrue(sdm != null);
          if (sdm.equals(vmdm)) {
            SystemMemberCache cache = s.getCache();
            assertNotNull(cache);
            return cache;
          }
        }
      }
      if (System.currentTimeMillis() >= deadLine)
        break;
      pause(2000);
    }
    return null;
  }
  
  /**
   * Tests accessing the hit count/miss count region statistics
   * via the admin API.  getLastAccessTime, getLastModifiedTime
   */
  public void testRegionAPIStatistics() throws Exception {
    VM vm = Host.getHost(0).getVM(0);

    final String name = this.getUniqueName() + "_root";

//    AdminDistributedSystem tc = this.tcSystem;
    vm.invoke(new CacheSerializableRunnable("Create cache") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setStatisticsEnabled(true);
          RegionAttributes attrs = factory.create();
          cache.createRegion(name, attrs);
        }
      });    

    SystemMemberCache cache = waitForCache(vm);
    cache.refresh();

    Set names = cache.getRootRegionNames();
    assertEquals(1, names.size());
    assertEquals(name, (String) names.iterator().next());
    
    SystemMemberRegion root = cache.getRegion(name);
    assertNotNull(root);

    assertEquals(0L, root.getHitCount());
    assertEquals(0L, root.getMissCount());

    final int entryCount = 5;
    vm.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Cache cache2 = CacheFactory.getAnyInstance();
          Region region = cache2.getRegion(name);
          for (int i = 0; i < entryCount; i++) {
            Object key = new Integer(i);
            Object value = String.valueOf(i);
            region.get(key);
            region.put(key, value);
            region.get(key);
            region.get(key);
          }
        }
      });

    root.refresh();
    assertEquals(entryCount, root.getEntryCount());
    assertEquals(entryCount * 2, root.getHitCount());
    assertEquals(entryCount, root.getMissCount());
    assertEquals((2.0f / 3.0f), root.getHitRatio(), 0.0f);

    long lastAccessed = root.getLastAccessedTime();
    long lastModified = root.getLastModifiedTime();

    vm.invoke(new CacheSerializableRunnable("Access region") {
        public void run2() throws CacheException {
          try {
            // Sleep to avoid problems with the 100-ms clock
            // granularity on Windows
            Thread.sleep(200);

          } catch (InterruptedException ex) {
            fail("Why was I interrupted?", ex);
          }

          Cache cache2 = CacheFactory.getAnyInstance();
          Region region = cache2.getRegion(name);
          for (int i = 0; i < entryCount; i++) {
            Object key = new Integer(i);
            region.get(key);
          }
        }
      });

    root.refresh();
    assertEquals(lastModified, root.getLastModifiedTime());
    assertTrue(lastAccessed < root.getLastAccessedTime());
  }

  /**
   * Tests administration operations on subregions
   */
  public void testSubregions() throws Exception {
    Host host = Host.getHost(0);

    final int rootRegionCount = 2;
    final int subregionCount = 4;
    final int entryCount = 10;

    for (int i = 0; i < host.getVMCount(); i++) {
      VM vm = host.getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create regions") {
        public void run2() throws CacheException {
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          RegionAttributes attrs = factory.create();
          for (int i2 = 0; i2 < rootRegionCount; i2++) {
            Region root = cache.createRegion("Root" + i2, attrs);
            for (int j = 0; j < entryCount; j++) {
              Object key = String.valueOf(j);
              Object value = new Integer(j);
              root.put(key, value);
            }

            for (int j = 0; j < subregionCount; j++) {
              String name = "Subregion-" + i2 + "-" + j;
              Region subregion = root.createSubregion(name, attrs);
              for (int k = 0; k < entryCount; k++) {
                Object key = String.valueOf(k);
                Object value = new Integer(k);
                subregion.put(key, value);
              }
            }
          }
        }
      });    
    }

    AdminDistributedSystem tc = this.tcSystem;

    SystemMember[] members = tc.getSystemMemberApplications();
    assertEquals(host.getVMCount(), members.length);
    
    for (int i = 0; i < host.getVMCount(); i++) {
      SystemMemberCache cache = waitForCache(host.getVM(i));
      assertNotNull(cache);
      cache.refresh();

      Set roots = cache.getRootRegionNames();
      assertEquals(rootRegionCount, roots.size());
      for (Iterator iter = roots.iterator(); iter.hasNext(); ) {
        String rootName = (String) iter.next();
        SystemMemberRegion root = cache.getRegion(rootName);
        assertNotNull(root);
        assertEquals(entryCount, root.getEntryCount());

        assertTrue(root.getName().startsWith("Root"));
        assertTrue(root.getFullPath() + " doesn't start with \"Root\"",
                   root.getFullPath().startsWith("/Root"));

        Set subregions = root.getSubregionNames();
        assertEquals(subregionCount, subregions.size());

        subregions = root.getSubregionFullPaths();
        for (Iterator iter2 = subregions.iterator();
             iter2.hasNext(); ) {
          String subPath = (String) iter2.next();
          SystemMemberRegion subregion = cache.getRegion(subPath);
          assertNotNull("Could not get subregion \"" + subPath + "\"",
                        subregion);
          assertEquals(entryCount, subregion.getEntryCount());

          assertTrue(subregion.getName().startsWith("Subregion"));
          assertTrue(subregion.getFullPath().startsWith("/Root"));
        }
      }
    }
  }

  /**
   * Test region cacheListener attribute with one cacheListener Verify that the
   * attribute returns the added CacheListener
   */
  public void testRegionAttributeCacheListener() throws Exception {
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");

    VM vm = Host.getHost(0).getVM(0);

    final String name = this.getUniqueName() + "_root";
    final Scope scope = Scope.DISTRIBUTED_ACK;

    final AdminDistributedSystem tc = this.tcSystem;
    vm.invoke(new CacheSerializableRunnable("Create cache") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.addCacheListener(new CacheListener2());
        factory.setScope(scope);

        RegionAttributes attrs = factory.create();
        Region region = cache.createRegion(name, attrs);
      }
    });

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return tc.getSystemMemberApplications().length == 1;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }

      public String description() {
        return "system member applications length never became exactly 1";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);

    SystemMember[] apps = tc.getSystemMemberApplications();
    if (apps.length != 1) {
      StringBuffer sb = new StringBuffer("Applications are ");
      for (int i = 0; i < apps.length; i++) {
        sb.append(apps[i]);
        sb.append(" ");
      }
      assertEquals(sb.toString(), 1, apps.length);
    }

    SystemMemberCache cache = waitForCache(vm);
    cache.refresh();

    Set names = cache.getRootRegionNames();
    assertEquals(1, names.size());
    assertEquals(name, (String)names.iterator().next());

    SystemMemberRegion root = cache.getRegion(name);
    assertNotNull(root);

    String expected = CacheListener2.class.getName();
    String actual = root.getCacheListener();
    if (!expected.equals(actual)) {
      String s = "Expected <" + expected + "> got <" + actual + ">";
      fail(s);
    }
  }

  /**
   * Tests region getCacheListeners method with multiple cacheListeners Verify
   * that cachelistener attribute/getCacheListener method returns the 1st added
   * CacheListener
   */
  public void testRegionMethodGetCacheListeners() throws Exception {
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");

    VM vm = Host.getHost(0).getVM(0);

    final String name = this.getUniqueName() + "_root";
    final Scope scope = Scope.DISTRIBUTED_ACK;

    final AdminDistributedSystem tc = this.tcSystem;
    vm.invoke(new CacheSerializableRunnable("Create cache") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        AttributesFactory factory = new AttributesFactory();
        factory.addCacheListener(new CacheListener2());
        factory.addCacheListener(new CacheListener1());
        factory.setScope(scope);

        RegionAttributes attrs = factory.create();
        Region region = cache.createRegion(name, attrs);
      }
    });

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return tc.getSystemMemberApplications().length == 1;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }

      public String description() {
        return "system member applications length never became exactly 1";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);

    SystemMember[] apps = tc.getSystemMemberApplications();
    if (apps.length != 1) {
      StringBuffer sb = new StringBuffer("Applications are ");
      for (int i = 0; i < apps.length; i++) {
        sb.append(apps[i]);
        sb.append(" ");
      }
      assertEquals(sb.toString(), 1, apps.length);
    }

    SystemMemberCache cache = waitForCache(vm);
    cache.refresh();

    Set names = cache.getRootRegionNames();
    assertEquals(1, names.size());
    assertEquals(name, (String)names.iterator().next());

    SystemMemberRegion root = cache.getRegion(name);
    assertNotNull(root);

    String[] expected = { CacheListener2.class.getName(),
        CacheListener1.class.getName() };
    int expectedSize = 2;
    String[] actuals = root.getCacheListeners();
    if (expectedSize != actuals.length) {
      String s = "Expected <" + expectedSize + "> got <" + actuals.length + ">";
      fail(s);
    }

    if (!(expected[0].equals(actuals[0]) || expected[0].equals(actuals[1]))) {
      String s = "Expected <" + expected[0] + "> got <" + actuals[0] + ", "
          + actuals[1] + ">";
      fail(s);
    }

    if (!(expected[1].equals(actuals[0]) || expected[1].equals(actuals[1]))) {
      String s = "Expected <" + expected[1] + "> got <" + actuals[0] + ", "
          + actuals[1] + ">";
      fail(s);
    }

    String actual = root.getCacheListener();
    String expectedOne = CacheListener2.class.getName();
    if (!expectedOne.equals(actual)) {
      String s = "Expected <" + expectedOne + "> got <" + actual + ">";
      fail(s);
    }
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * A no-op cache loader
   */
  static class CacheLoader2 implements CacheLoader {
    public Object load(LoaderHelper helper)
      throws CacheLoaderException {

      return null;
    }

    public void close() {

    }
  }

  static class CacheListener2 extends CacheListenerAdapter {

  }

  static class CacheWriter2 extends CacheWriterAdapter {

  }

  static class CacheListener1  extends CacheListenerAdapter {
  }
}
