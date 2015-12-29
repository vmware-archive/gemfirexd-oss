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

import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;

import dunit.Host;
import dunit.VM;

/**
 * This is a DUnit test to verify clean up of statistic JMX resources created 
 * for cache & application member VMs when these member VM gets disconnected.
 * 
 * <pre>
 * 1. Start agent from current client VM (VM#4) 
 * 2. From client VM, invoke connectToSystem on agent VM. Collect reference to 
 *    distributed system MBean in setUp. 
 * 3. Start the cache & initialize the system in cache VM (CACHE_VM i.e. VM#0). 
 * 4. From client VM, invoke manageSystemMemberApplications on distributed 
 *    system MBean (in step 2). On the SystemMember manageStat methods is called 
 *    over JMX. 
 * 5. Stop the cache & disconnect the cacheVM from the DS. 
 * 6. Checks if the count of all the created managed statistics resources has 
 *    become zero again.
 * </pre>
 * 
 * @author abhishek
 */
public class JmxStatResourcesCleanupDUnitTest extends AdminDUnitTestCase {

  private static final long serialVersionUID = 1L;
  
  /** index for the VM to be used as a Cache VM */
  private static final int CACHE_VM = 0;

  /** log writer instance */
  private static LogWriter logWriter = getLogWriter();

  /** pause timeout */
  private static int PAUSE_TIME = 15;
  
  private static ObjectName distributedSystem;
  private static MBeanServerConnection mbsc;

  public JmxStatResourcesCleanupDUnitTest(String name) {
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
    logWriter.fine("Entered JmxStatResourcesCleanupDUnitTest.testCleanUp");
    logWriter.fine("JmxStatResourcesCleanupDUnitTest.testCleanUp#urlString :: "
                   + this.urlString);
    
    startCache();
    pause(1000*PAUSE_TIME);
    manageSystemMembers(this.urlString);

    logWriter.info("JmxStatResourcesCleanupDUnitTest.testCleanUp : " +
    		           "After manageSystemMembers and before stopCache: " +
    		           "stats resource count :: " + getStatResourceCount());
    
    stopCache();

    WaitCriterion wc = new WaitCriterion() {
      public String description() {
        return "Wait for clean up to be done ...";
      }

      public boolean done() {
        try {
          int statResourceCount = getStatResourceCount();
          return statResourceCount == 0;
        } catch (Exception e) {
          return true;
        }
      }
    };
    waitForCriterion(wc, 30000, 1000, false);

    logWriter.info("JmxStatResourcesCleanupDUnitTest.testCleanUp : " +
    		           "After stopCache: stats resource count :: " + 
                   getStatResourceCount());
    
    boolean cleanUpDone = checkCleanUp();
    assertTrue("MBeans clean up wasn't done properly.", cleanUpDone);

    logWriter.fine("Exited JmxStatResourcesCleanupDUnitTest.testCleanUp");
  }
  
  /**
   * Starts the cache in the cache VM at index CACHE_VM.
   * Also initializes the distributed system in cache VM.
   * 
   * NOTE: This test case extends CacheTestCase. getCache() method from this 
   * class is called to create the cache. 
   */
  private void startCache() {
    logWriter.fine("Entered JmxStatResourcesCleanupDUnitTest.startCache");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-startCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        /*
         * 1. getCache creates & returns a cache if not present currently.
         * 2. While creating the cache initializes the systems if currently not 
         *    connected to the DS or was disconnected from the DS earlier.
         */        
        getCache();
        logWriter.info("JmxStatResourcesCleanupDUnitTest.startCache :: " +
        		           "Cache created in cacheVM(VM#"+CACHE_VM+").");
      }
    });
    
    logWriter.fine("Exited JmxStatResourcesCleanupDUnitTest.startCache");
  }

  /**
   * Calls manageSystemMemberApplications on the AdminDistributedSystem using 
   * the ObjectName cached in setUp call. The call is done over JMX.
   * 
   * The manageSystemMemberApplications call returns an array of ObjectNames for 
   * SystemMembers. On each SystemMember manageStat is called over JMX.  
   * 
   * @param urlString url string to get the MBeanServerConnection
   * @throws Exception if JMX operation fails
   */
  private static void manageSystemMembers(String urlString) throws Exception {
    logWriter
        .fine("Entered JmxStatResourcesCleanupDUnitTest.manageSystemMembers");

    String[] params = {}, signature = {};
    ObjectName[] memberApplications = (ObjectName[]) mbsc.invoke(
        distributedSystem, "manageSystemMemberApplications", params, signature);

    assertNotNull(
        "manageSystemMemberApplications invocation results are null.",
        memberApplications);
    assertEquals("manageSystemMemberApplications invocation returned "
        + memberApplications.length + " while expected is 1", 1,
        memberApplications.length);
    logWriter.info("JmxStatResourcesCleanupDUnitTest.manageSystemMembers : "
        + "Total Member Apps :: " + memberApplications.length);

    ObjectName systemMemberApp = memberApplications[0];
    mbsc.invoke(systemMemberApp, "manageStats", params, signature);

    logWriter
        .fine("Exited JmxStatResourcesCleanupDUnitTest.manageSystemMembers");
  }
  
  /**
   * Stops the cache in the cache VM & disconnects the cache VM from the DS.
   */
  private void stopCache() {
    logWriter.fine("Entered JmxStatResourcesCleanupDUnitTest.stopCache");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-stopCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
      }
    });
    logWriter.fine("Exited JmxStatResourcesCleanupDUnitTest.stopCache");
  }
  
  /**
   * Returns the count of all the managed statistic resources under domain 
   * 'GemFire.Statistic'. 
   *  
   * @return Count of the managed statistic resources
   * @throws Exception if JMX operation fails
   */
  private static int getStatResourceCount() throws Exception {
    logWriter.fine("Entered JmxStatResourcesCleanupDUnitTest.getStatResourceCount");
    
    String domainString = "GemFire.Statistic:*";
    Set queryMBeans = mbsc.queryNames(null, new ObjectName(domainString));
    assertNotNull("Query on beans under domain '" + domainString
                  + "' returned null.", queryMBeans);
    
    logWriter.fine("Exiting JmxStatResourcesCleanupDUnitTest.getStatResourceCount");
    return queryMBeans.size();
  }

  /**
   * Checks if the clean up of resources is done as expected. Checks if the
   * count of all the managed statistic resources that were created has become
   * zero.
   * Internally calls getStatResourceCount()
   * 
   * @return true if all the created managed statistic resources were cleaned
   *         up, false otherwise
   * @throws Exception if JMX operation fails
   */
  private boolean checkCleanUp() throws Exception {
    logWriter.fine("Entered JmxStatResourcesCleanupDUnitTest.checkCleanUp");
    
    boolean cleanUpDone = getStatResourceCount() == 0;
    
    logWriter.fine("Exiting JmxStatResourcesCleanupDUnitTest.checkCleanUp");
    return cleanUpDone;
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
