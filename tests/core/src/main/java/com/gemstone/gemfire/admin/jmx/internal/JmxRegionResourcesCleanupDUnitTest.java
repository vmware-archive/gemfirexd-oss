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
package com.gemstone.gemfire.admin.jmx.internal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.RegionSubRegionSnapshot;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;

import dunit.Host;
import dunit.VM;

/**
 * This is a DUnit test to verify clean up of region JMX resources created 
 * for cache & application member VMs when the region(s) get destroyed/removed.
 * 
 * <pre>
 * 1. Start agent from current client VM (VM#4) 
 * 2. From client VM, invoke connectToSystem on agent VM. Collect reference to 
 *    distributed system MBean in setUp. 
 * 3. Start the cache & initialize the system in cache VM (CACHE_VM i.e. VM#0).
 * 4. From client VM, invoke manageSystemMemberApplications on distributed 
 *    system MBean (in step 2). On the SystemMember manageStat methods is called 
 *    over JMX.
 * 5. Create a region region_1 in the cache VM. Count number number of region 
 *    JMX resources in JMX client VM.
 * 6. Create a region region_2 in the cache VM. Count number number of region 
 *    JMX resources in JMX client VM.     
 * 7. Remove region_1 & region_2 in the cache VM. Count number number of region 
 *    JMX resources in JMX client VM. The count should be zero (same as before 
 *    creating region_1).
 * 8. Stop the cache & disconnect the cacheVM from the DS.
 * </pre>
 * 
 * @author abhishek
 */
public class JmxRegionResourcesCleanupDUnitTest extends AdminDUnitTestCase {
  private static final long serialVersionUID = 1L;
  
  /** index for the VM to be used as a Cache VM */
  private static final int CACHE_VM = 0;

  protected static final String REGION_NAME1 = "JmxRegionResourcesCleanupDUnitTest_REGION1";
  protected static final String REGION_NAME2 = "JmxRegionResourcesCleanupDUnitTest_REGION2";

  /** log writer instance */
  private static LogWriter logWriter = getLogWriter();

  /** pause timeout */
  private static int PAUSE_TIME = 15;
  
  /* useful for JMX client VM (i.e. controller VM - VM4) */
  private static ObjectName distributedSystem;
  private static MBeanServerConnection mbsc;
  
  /** map to store region paths against region names */
  /* Having this variable static ensures accessing this map across multiple 
   * invoke operations in that VM. Here it's used in CACHE_VM */
  protected static final Map<String, String> regionPaths = new HashMap<String, String>();

  private static final Properties props = new Properties();

  public JmxRegionResourcesCleanupDUnitTest(String name) {
    super(name);
  }

  /**
   * Whether JMX is to be used in this test case. Overridden to return 
   * <code>true</code>.
   * 
   * @return true always
   */
  @Override
  protected boolean isJMX() {
    return true;
  }
  
  /**
   * Test to check clean up of managed stats resources.
   * 
   * @throws Exception one of the reasons could be failure of JMX operations 
   */
  public void testCleanUp() throws Exception {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.testCleanUp");
    logWriter.fine("JmxRegionResourcesCleanupDUnitTest.testCleanUp#urlString :: "
                   + this.urlString);
    initDSProperties();
    
    startCache();
    pause(1000*PAUSE_TIME);
    ObjectName memberObjectName = manageSystemMembers();
    ObjectName cacheObjectName  = manageCache(memberObjectName);
    
    manageRegions(cacheObjectName);    
    int count0 = getRegionResourceCount();
    logWriter.info("No. of region JMX resources - initial count :: "+ count0);
    
    createRegion(REGION_NAME1);
    manageRegions(cacheObjectName);    
    int count1 = getRegionResourceCount();
    logWriter.info("No. of region JMX resources - after creating 1 region - count :: "+ count1);
    
    createRegion(REGION_NAME2);
    manageRegions(cacheObjectName);
    int count2 = getRegionResourceCount();
    logWriter.info("No. of region JMX resources - after creating 2 regions - count :: "+ count2);
    
    pause(500*PAUSE_TIME);
    destroyRegion(REGION_NAME1);
    destroyRegion(REGION_NAME2);
    pause(500*PAUSE_TIME);
    int count3 = getRegionResourceCount();
    logWriter.info("No. of region JMX resources - after destroying all(2) regions - count :: "+ count3);
    
    assertTrue("Region resources were not removed.", count3 == count0);
    
    stopCache();

    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.testCleanUp");
  }  

  /**
   * Over-ridden to ensure that all the members connect to the same distributed 
   * system. Distributed system properties used would be same for all the 
   * members that use this method to get the properties. 
   */
  @Override
  public Properties getDistributedSystemProperties() {
    return props;
  }
  
  /**
   * Initializes the properties for the distributed system to be started. 
   */
  public static void initDSProperties() {
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.clear();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastPort));
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
  }
  
  /**
   * Starts the cache in the cache VM at index CACHE_VM.
   * Also initializes the distributed system in cache VM, starts a server and
   * creates a region with name {@link #REGION_NAME}.
   * 
   * NOTE: This test case extends CacheTestCase. getCache() method from this 
   * class is called to create the cache.
   */
  private void startCache() {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.startCache");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-startCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        /* [Refer com.gemstone.gemfire.cache30.CacheTestCase] 
         * a. getCache creates & returns a cache if not present currently.
         * b. While creating the cache initializes the systems if currently not 
         *    connected to the DS or was disconnected from the DS earlier.
         */
        Cache cacheInstance = getCache();        
        assertNotNull("Cache was not created.", cacheInstance);
        
        logWriter.info("JmxRegionResourcesCleanupDUnitTest.startCache :: " + 
            "Created cache & started server in cacheVM(VM#"+CACHE_VM+")");
      }
    });
    
    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.startCache");
  }
  
  private void createRegion(final String regionName) {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.createRegion");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-createRegion-"+regionName) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Cache cacheInstance = getCache();
        
        AttributesFactory<String, String> factory = 
                        new AttributesFactory<String, String>();
        RegionAttributes<String, String> attributes = factory.create();
        
        Region<String,String> region = cacheInstance.createRegion(regionName, attributes);
        assertNotNull("Region with name '"+regionName+"' was not created", region);
        
        regionPaths.put(regionName, region.getFullPath());
        
        logWriter.info("JmxRegionResourcesCleanupDUnitTest.createRegion :: " + 
            "Created region: '" + region.getFullPath() + "' in cacheVM(VM#" + 
            CACHE_VM + ")");
      }
    });
    
    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.createRegion");
  }
  
  private void destroyRegion(final String regionName) {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.destroyRegion");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-destroyRegion-"+regionName) {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Cache cacheInstance = getCache();
        
        String regionPath = regionPaths.get(regionName);
        
        assertNotNull("Regin Path not known for region name:"+regionName);
        
        Region<String,String> region = cacheInstance.getRegion(regionPath);
        region.destroyRegion();
        
        logWriter.info("JmxRegionResourcesCleanupDUnitTest.destroyRegion :: " + 
            "Destroyed region: '" + region.getFullPath() + "' in cacheVM(VM#" + 
            CACHE_VM + ")");
      }
    });
    
    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.destroyRegion");
  }

  /**
   * Calls manageSystemMemberApplications on the AdminDistributedSystem using 
   * the ObjectName cached in setUp call. The call is done over JMX.
   * 
   * The manageSystemMemberApplications call returns an array of ObjectNames for 
   * SystemMembers. On each SystemMember manageStat is called over JMX.  
   * 
   * @return returns the ObjectName for the MBean representing a server 
   *         started in {@link #CACHE_VM}.
   * @throws Exception if JMX operation fails
   */
  private static ObjectName manageSystemMembers() throws Exception {
    logWriter
        .fine("Entered JmxRegionResourcesCleanupDUnitTest.manageSystemMembers");

    String[] params = {}, signature = {};
    ObjectName[] memberApplications = (ObjectName[]) mbsc.invoke(
        distributedSystem, "manageSystemMemberApplications", params, signature);

    assertNotNull(
        "manageSystemMemberApplications invocation results are null.",
        memberApplications);
    assertEquals("manageSystemMemberApplications invocation returned "
        + memberApplications.length + " while expected is 1", 1,
        memberApplications.length);
    logWriter.info("JmxRegionResourcesCleanupDUnitTest.manageSystemMembers : "
        + "Total Member Apps :: " + memberApplications.length);
    logWriter
        .fine("Exited JmxRegionResourcesCleanupDUnitTest.manageSystemMembers");
    
    return memberApplications[0];
  }

  /**
   * Invokes JMX operation manageCache available for SystemMember to
   * initialize/update the cache JMX resources.
   * 
   * @param member
   *          ObjectName for the MBean representing a server started in
   *          {@link #CACHE_VM}
   * @return ObjectName for the MBean representing a cache on a server
   * @throws Exception
   *           if invoking the JMX operation fails.
   */
  private static ObjectName manageCache(ObjectName member) throws Exception {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.manageCache");
    String[] params = {}, signature = {};
    ObjectName cache = (ObjectName) mbsc.invoke(member, "manageCache", params, signature);

    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.manageCache");
    return cache;
  }
  
  /**
   * Invokes JMX operation manageRegions available for SystemMemberCache to
   * initialize/update the region JMX resources.
   * 
   * @param cacheON
   *          ObjectName for the MBean representing a server started in
   *          {@link #CACHE_VM}
   * @return ObjectName for the MBean representing a cache on a server
   * @throws Exception
   *           if invoking the JMX operation fails.
   */
  @SuppressWarnings("unchecked")
  //RegionSubRegionSnapshot API needs to add types used to collection returned.
  private static void manageRegions(ObjectName cacheON) throws Exception {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.manageRegions");
    String[] params = {}, signature = {};
    RegionSubRegionSnapshot regionsSnap = (RegionSubRegionSnapshot) 
                  mbsc.invoke(cacheON, "getRegionSnapshot", params, signature);
    
    assertNotNull("Regions Snapshot is null.", regionsSnap);
    
    // The root region path /ROOT
    String rootPath = regionsSnap.getFullPath();
    
    Set subRegionSnapshots = regionsSnap.getSubRegionSnapshots();    
    params    = new String[1];
    signature = new String[] {"java.lang.String"};

    for (Iterator iterator = subRegionSnapshots.iterator(); 
         iterator.hasNext();) {
      RegionSubRegionSnapshot subRegionSnap = 
                                    (RegionSubRegionSnapshot) iterator.next();
      /*
       * We create a convenient region root node - /ROOT.
       * The full path for regions also contains the path element created for 
       * this root region which won't be present in actual cache. Hence, 
       * removing it out from the full region path.
       */
      String fullPath = subRegionSnap.getFullPath();
      fullPath = fullPath.substring(rootPath.length());
      
      params[0] = fullPath;
      mbsc.invoke(cacheON, "manageRegion", params, signature);
    }

    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.manageRegions");
  }  
  
  /**
   * Stops the cache in the cache VM & disconnects the cache VM from the DS.
   */
  private void stopCache() {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.stopCache");
    Host host    = Host.getHost(0);
    VM   cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-stopCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
      }
    });
    logWriter.fine("Exited JmxRegionResourcesCleanupDUnitTest.stopCache");
  }
  
  /**
   * Returns the count of all the managed statistic resources under domain 
   * 'GemFire.Statistic'. 
   *  
   * @return Count of the managed statistic resources
   * @throws Exception if JMX operation fails
   */
  private static int getRegionResourceCount() throws Exception {
    logWriter.fine("Entered JmxRegionResourcesCleanupDUnitTest.getRegionResourceCount");
    
    String domainString = "GemFire.Cache:*,type=Region";
    Set<ObjectName> queryMBeans = mbsc.queryNames(null, new ObjectName(domainString));
    assertNotNull("Query on beans under domain '" + domainString
                  + "' returned null.", queryMBeans);
    
    logWriter.fine("Exiting JmxRegionResourcesCleanupDUnitTest.getRegionResourceCount");
    return queryMBeans.size();
  }

  public void setUp() throws Exception {    
    disconnectAllFromDS();
    super.setUp();

    // Calls connectToSystem on the JMX agent over JMX.
    // The distributed system MBean ObjectName is collected/cached in a variable
    mbsc = this.agent.getMBeanServer();
    assertNotNull(mbsc);

    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    distributedSystem = (ObjectName)mbsc.invoke(agentName, "connectToSystem",
        new Object[0], new String[0]);
    assertNotNull(distributedSystem);
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();    
    disconnectAllFromDS();
  }
}
