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

import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.SystemMemberCache;
import com.gemstone.gemfire.admin.internal.SystemMemberCacheImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.RegionStatus;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests the functionality of the {@link RegionStatus} object used both in
 * GFMonI and II for core status
 * 
 * @author Harsh Khanna
 * @since 5.7
 */
public class GemFireMemberRegionStatusDUnitTest extends AdminDUnitTestCase {
  public static final int MEM_VM = 0;

  private final int noOfEntity = 10;

  /**
   * Creates a new <code>GemFireMemberStatusDUnitTest</code>
   */
  public GemFireMemberRegionStatusDUnitTest(String name) {
    super(name);
  }

  // ////// Test Methods

  /**
   * Tests utilities to create a member
   */
  protected VM getMemberVM() {
    Host host = Host.getHost(0);
    return host.getVM(MEM_VM);
  }

  protected SystemMemberCache getMemberCache() throws Exception {
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

    SystemMemberCache cache = members[0].getCache();
    assertNull(cache);
    vm.invoke(new CacheSerializableRunnable("Create cache") {
      public void run2() throws CacheException {
        CacheFactory.create(getSystem());
      }
    });

    getLogWriter().info("Test: Created Cache");
    cache = members[0].getCache();
    assertNotNull(cache);

    return cache;
  }

  /**
   * Tests that status object serialized properly by looking at various
   * attributed
   */
  public void testStatusSerialization() throws Exception {
    // Get the cache
    SystemMemberCache cache = getMemberCache();

    // Get the Status Object, verify not null & not a server
    GemFireMemberStatus status = ((SystemMemberCacheImpl)cache).getSnapshot();
    assertNotNull(status.getRegionStatuses());
  }

  /**
   * Tests that status object serialized properly by looking at various
   * attributes for a root region
   */
  public void testStatusSerializationForRootRegion() throws Exception {
    // Create Member an Cache
    SystemMemberCache cache = getMemberCache();

    VM vm1 = getMemberVM();

    vm1.invoke(new CacheSerializableRunnable("Create cache") {
      public void run2() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();

        AttributesFactory factory = new AttributesFactory();
        MembershipAttributes membershipAttributes = new MembershipAttributes(
            new String[] { "A" }, LossAction.FULL_ACCESS, ResumptionAction.NONE);
        factory.setMembershipAttributes(membershipAttributes);
        RegionAttributes attrs = factory.create();

        Region region = cache.createRegion("RegionTest", attrs);
        Random generator = new Random();
        int temp;
        for (int i = 0; i < noOfEntity; i++) {
          temp = generator.nextInt(200);
          region.put(String.valueOf(i), new Integer(temp));
        }
        getLogWriter().info("Created a Region " + region.getFullPath());
      }
    });

    // Get the Status Object, verify not null & not a server
    GemFireMemberStatus status = ((SystemMemberCacheImpl)cache).getSnapshot();
    assertNotNull(status);
    assertNotNull(status.getRegionStatuses());
    RegionStatus regionStatus = (RegionStatus)status.getRegionStatus("/RegionTest");
    assertNotNull(regionStatus);
    assertEquals(noOfEntity, regionStatus.getNumberOfEntries());
    assertNotNull(regionStatus.toString());
  }

  /**
   * Tests that status object serialized properly by looking at various
   * attributes for a subregions
   */
  public void testStatusSerializationForSubRegions() throws Exception {
    // Create Member an Cache
    SystemMemberCache cache = getMemberCache();

    VM vm1 = getMemberVM();

    vm1.invoke(new CacheSerializableRunnable("Create cache") {
      public void run2() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();

        AttributesFactory factory = new AttributesFactory();
        MembershipAttributes membershipAttributes = new MembershipAttributes(
            new String[] { "A" }, LossAction.FULL_ACCESS, ResumptionAction.NONE);
        factory.setMembershipAttributes(membershipAttributes);
        RegionAttributes attrs = factory.create();

        Region region = cache.createRegion("RegionTest", attrs);
        Region subregion = region.createSubregion("SubRegionTest", attrs);
        Random generator = new Random();
        int temp;
        for (int i = 0; i < noOfEntity; i++) {
          temp = generator.nextInt(200);
          subregion.put(String.valueOf(i), new Integer(temp));
        }
        getLogWriter().info("Created a Region " + region.getFullPath());
        getLogWriter().info("Created a SubRegion " + subregion.getFullPath());
      }
    });

    // Get the Status Object, verify not null & not a server
    GemFireMemberStatus status = ((SystemMemberCacheImpl)cache).getSnapshot();
    assertNotNull(status);
    assertNotNull(status.getRegionStatuses());
    RegionStatus regionStatus = (RegionStatus)status
        .getRegionStatus("/RegionTest/SubRegionTest");
    assertNotNull(regionStatus);
    assertEquals(noOfEntity, regionStatus.getNumberOfEntries());
    assertNotNull(regionStatus.toString());
  }

}
