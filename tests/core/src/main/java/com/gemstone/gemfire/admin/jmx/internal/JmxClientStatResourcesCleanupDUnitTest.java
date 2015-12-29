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

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;

import dunit.Host;
import dunit.VM;

/**
 * This is a DUnit test to verify clean up of client statistic JMX resources 
 * created in cache & application member VMs when a client gets disconnected.
 * 
 * <pre>
 * For testCleanUp:
 * 
 * 1. Start agent from current JMX client VM (VM#4) 
 * 2. From JMX client VM, invoke connectToSystem on agent VM. Collect reference
 *    to distributed system MBean in setUp. 
 * 3. Start the cache & initialize the system in cache VM (CACHE_VM i.e. VM#0).
 * 4. From JMX client VM, invoke manageSystemMemberApplications on distributed 
 *    system MBean (in step 2). On the SystemMember manageStat method is called 
 *    over JMX.
 * 5. Count number of JMX statistics resources. 
 * 6. Start & connect a cache client (CLIENT_VM i.e. VM#2) to the cache VM.   
 * 7. Invoke operation manageStat on the SystemMember. Count number of JMX 
 *    statistics resources.
 * 8. Stop the client & disconnect from the cacheVM.
 * 9. Invoke operation manageStat on the SystemMember. Count number of JMX 
 *    statistics resources, it should be same as in step 5.
 *    
 * For testClientMembership: 
 * 1. Start agent from current JMX client VM (VM#4) 
 * 2. From JMX client VM, invoke connectToSystem on agent VM. Collect reference
 *    to distributed system MBean in setUp. 
 * 3. Start the cache & initialize the system in cache VM (CACHE_VM i.e. VM#0).
 * 4. From JMX client VM, invoke manageSystemMemberApplications on distributed 
 *    system MBean (in step 2). On the SystemMember manageStat method is called 
 *    over JMX. Register Notification Listener on this SystemMember MBean.
 * 5. Count number of Notifications. 
 * 6. Start & connect a cache client (CLIENT_VM i.e. VM#2) to the cache VM.   
 * 7. Invoke operation manageStat on the SystemMember. Count number 
 *    Notifications, it should be more than in step 5.
 * 8. Stop the client & disconnect from the cacheVM.
 * 9. Invoke operation manageStat on the SystemMember. Count number of JMX 
 *    Notifications, it should be more than in step 7.
 * </pre>
 * 
 * @author abhishek
 */
public class JmxClientStatResourcesCleanupDUnitTest extends AdminDUnitTestCase {

  private static final long serialVersionUID = 1L;
  
  /** index for the VM to be used as a Cache VM */
  private static final int CACHE_VM = 0;
  
  /** index for the VM to be used as a Cache Client VM */
  private static final int CLIENT_VM = 2;

  protected static final String REGION_NAME = "JmxClientStatResourcesCleanupDUnitTest_REGION";

  /** log writer instance */
  private static LogWriter logWriter = getLogWriter();

  /** pause timeout */
  private static int PAUSE_TIME = 15;
  
  /* useful for JMX client VM (i.e. controller VM - VM4) */
  private static ObjectName distributedSystem;
  private static MBeanServerConnection mbsc;
  
//  private static int notificationCount = 0;
  private static AtomicInteger notificationCount = new AtomicInteger();
  
  /** reference to cache client connection to cache server in VM0*/
  /* Having this variable static ensures accessing this map across multiple 
   * invoke operations in that VM. Here it's used in CLIENT_VM */
  private static Connection clientConn;

  private static final Properties props = new Properties();

  private int mcastPort;

  public JmxClientStatResourcesCleanupDUnitTest(String name) {
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
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.testCleanUp");
    logWriter.fine("JmxClientStatResourcesCleanupDUnitTest.testCleanUp#urlString :: "
                   + this.urlString);
    int serverPort = 
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    startCache(mcastPort, serverPort);
    pause(1000*PAUSE_TIME);
    ObjectName member = manageSystemMembers();
    manageStats(member);

    int count0 = getStatResourceCount();
    
    /* START Remove this block once bug #41616 is fixed. */
    connectClient(serverPort);
    pause(500*PAUSE_TIME);
    manageStats(member); 
    disConnectClient();
    pause(500*PAUSE_TIME);
    manageStats(member); 
    count0 = getStatResourceCount();
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testCleanUp : " +
        "After manageSystemMembers and before stopCache: " +
        "stats resource count0 :: " + count0);
    
    connectClient(serverPort);    
    /* END */

//    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testCleanUp : " +
//        "After manageSystemMembers and before stopCache: " +
//        "stats resource count :: " + count0);
//    connectClient(serverPort);

    pause(500*PAUSE_TIME);
    manageStats(member); 
    int count1 = getStatResourceCount();
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testCleanUp# count1 : " +count1);
    assertTrue("Client Stat resources were not created", count1 > count0);
    
    disConnectClient();
    
    pause(1000*PAUSE_TIME);
    manageStats(member);
    final int count0_2 = count0;
    WaitCriterion wc = new WaitCriterion() {
      public String description() {
        return "Wait for clean up to be done ...";
      }

      public boolean done() {
        try {
          int statResourceCount = getStatResourceCount();
          return statResourceCount == count0_2;
        } catch (Exception e) {
          return true;
        }
      }
    };
    waitForCriterion(wc, 30000, 1000, false);

    int count2 = getStatResourceCount();
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testCleanUp# count2 : " +count2);
    assertTrue("Client Stat resources were not removed.", count2 == count0);
    
    stopCache();

    logWriter.fine("Exited JmxClientStatResourcesCleanupDUnitTest.testCleanUp");
  }
  
  /**
   * Test to check notifications received as the client membership changes.
   * 
   * @throws Exception one of the reasons could be failure of JMX operations 
   */
  public void testClientMembership() throws Exception {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.testClientMembership");
    
    int serverPort = 
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    startCache(mcastPort, serverPort);
    pause(1000*PAUSE_TIME);
    ObjectName member = manageSystemMembers();
    addClientNotifListener(member);
    manageStats(member);

    int count0 = getNotificationCount(); //expected count=0

    connectClient(serverPort);
    pause(500*PAUSE_TIME);
    manageStats(member); 
    disConnectClient();
    pause(500*PAUSE_TIME);
    manageStats(member); 
    
    int count1 = getNotificationCount();  //expected count=2
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testClientMembership# count1 : " + count1);
    assertTrue("Client Membership Notifications were not received.", count1 > count0);
    
    connectClient(serverPort);

    pause(500*PAUSE_TIME);
    manageStats(member); 
    int count2 = getNotificationCount(); //expected count=3
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testClientMembership# count2 : " +count2);
    assertTrue("Client Membership Notifications were not received.", count2 > count1);
    
    disConnectClient();
    
    pause(1000*PAUSE_TIME);
    manageStats(member);

    int count3 = getNotificationCount();  //expected count=4
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.testClientMembership# count3 : " +count3);
    assertTrue("Client Membership Notifications were not received.", count3 > count2);
    
    stopCache();

    logWriter.fine("Exited JmxClientStatResourcesCleanupDUnitTest.testClientMembership");
  }

  /**
   * Adds notification listener to MBeanServerConnection to listen
   * 
   * @param objName
   *          ObjectName of the MBean that'll emit the notifications
   * @throws Exception
   *           if adding notification listener fails
   */
  private static void addClientNotifListener(ObjectName objName) throws Exception {
    mbsc.addNotificationListener(
        objName, 
        new NotificationListener() {
          public void handleNotification(Notification notification, Object handback) {
            notificationCount.incrementAndGet();
          }
        }, 
        new NotificationFilter() {
          private static final long serialVersionUID = 1L;
          public boolean isNotificationEnabled(Notification notification) {
            boolean isThisNotificationEnabled = false;
            if (notification.getType().equals(SystemMemberJmx.NOTIF_CLIENT_JOINED) ||
                notification.getType().equals(SystemMemberJmx.NOTIF_CLIENT_LEFT) || 
                notification.getType().equals(SystemMemberJmx.NOTIF_CLIENT_CRASHED) ) {
              isThisNotificationEnabled = true;
            }
            return isThisNotificationEnabled;
          }
        }, 
        null);
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
   * 
   * @param mcastPort
   *          mcast-port to use for setting up DS
   */
  public static void initDSProperties(int mcastPort) {
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
   * 
   * @param serverPort a port to start server on.
   */
  private void startCache(final int mcastPort, final int serverPort) {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.startCache");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-startCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        initDSProperties(mcastPort);
        /* [Refer com.gemstone.gemfire.cache30.CacheTestCase] 
         * a. getCache creates & returns a cache if not present currently.
         * b. While creating the cache initializes the systems if currently not 
         *    connected to the DS or was disconnected from the DS earlier.
         */
        //1. Start Cache
        Cache cacheInstance = getCache();
        
        //2. Create Region Attributes
        AttributesFactory<String, String> factory = 
                                        new AttributesFactory<String, String>();
        RegionAttributes<String, String> attrs = factory.create();
        
        //3. Create Region
        cacheInstance.createRegion(REGION_NAME, attrs);
        
        //4. Create & Start Server
        CacheServer server = cacheInstance.addCacheServer();
        server.setPort(serverPort);
        try {
          server.start();
        } catch (IOException e) {
          throw new CacheException("Exception occurred while starting server.", e) {//TODO: Revisit this.
            private static final long serialVersionUID = 1L;
          };
        }
        logWriter.info("JmxClientStatResourcesCleanupDUnitTest.startCache :: " + 
            "Created cache & started server in cacheVM(VM#"+CACHE_VM+") at port:"+serverPort);
      }
    });
    
    logWriter.fine("Exited JmxClientStatResourcesCleanupDUnitTest.startCache");
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
        .fine("Entered JmxClientStatResourcesCleanupDUnitTest.manageSystemMembers");

    String[] params = {}, signature = {};
    ObjectName[] memberApplications = (ObjectName[]) mbsc.invoke(
        distributedSystem, "manageSystemMemberApplications", params, signature);

    assertNotNull(
        "manageSystemMemberApplications invocation results are null.",
        memberApplications);
    assertEquals("manageSystemMemberApplications invocation returned "
        + memberApplications.length + " while expected is 1", 1,
        memberApplications.length);
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.manageSystemMembers : "
        + "Total Member Apps :: " + memberApplications.length);
    logWriter
        .fine("Exited JmxClientStatResourcesCleanupDUnitTest.manageSystemMembers");
    
    return memberApplications[0];
  }
  
  /**
   * Invokes JMX operation manageStats available for SystemMember to 
   * initialize/update the statistics JMX resources.
   * 
   * @param member ObjectName for the MBean representing a server
   * @throws Exception if invoking the JMX operation fails.
   */
  private static void manageStats(ObjectName member) throws Exception {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.manageStats");
    String[] params = {}, signature = {};
    mbsc.invoke(member, "manageStats", params, signature);

    logWriter.fine("Exited JmxClientStatResourcesCleanupDUnitTest.manageStats");
  }

  /**
   * Connects a cache client in {@link #CLIENT_VM} to the server in
   * {@link #CACHE_VM} on the server port provided.
   * 
   * @param serverPort
   *          server port for clients to connect to
   * @return reference to the connection
   */
  private void connectClient(final int serverPort) {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.connectClient");
    final Host host     = Host.getHost(0);
    VM         clientVM = host.getVM(CLIENT_VM);
    
    clientVM.invoke(new CacheSerializableRunnable(getName()+"-connectClient") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        //1. Create Cache & DS if does not exist
        /* 
         * Creating loner cache would override properties defined in 
         * getDistributedSystemProperties() to use mcast-port=0 & locators="" 
         * as required for clients. 
         */
        createLonerCache();
        Cache cacheInstance = getCache();
        
        //2. Configure & create pool
        PoolFactoryImpl pf = (PoolFactoryImpl)PoolManager.createFactory();
        pf.addServer(host.getHostName(), serverPort);
        pf.setSubscriptionEnabled(true);
        pf.setReadTimeout(10000);
        pf.setSubscriptionRedundancy(0);
        
        PoolImpl p = (PoolImpl) pf.create("JmxClientStatResourcesCleanupDUnitTest");
        
        //3. Create Region Attributes
        AttributesFactory<String, String> factory = 
                                        new AttributesFactory<String, String>();
        factory.setPoolName(p.getName());
        factory.setScope(Scope.LOCAL);
        RegionAttributes<String, String> attrs = factory.create();
        
        //4. Create region
        Region<String,String> region = cacheInstance.createRegion(REGION_NAME, attrs);        
        assertNotNull("Region in cache is is null.", region);

        clientConn = p.acquireConnection();
        assertNotNull("Acquired client connecttion is null.", clientConn);
        
      }
    });
    
    logWriter.info("JmxClientStatResourcesCleanupDUnitTest.connectClient :: " + 
        "Started client in clientVM(VM#"+CLIENT_VM+") & connected to " +
    		"cacheVM(VM#"+CACHE_VM+").");
  }
  
  /**
   * Dis-connects a cache client started in {@link #CLIENT_VM} from a cache 
   * server started in {@link #CACHE_VM}. Also closes the cache in client and 
   * destroys the connection.
   */
  private void disConnectClient() {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.stopCache");
    Host host     = Host.getHost(0);
    VM   clientVM = host.getVM(CLIENT_VM);
    
    clientVM.invoke(new CacheSerializableRunnable(getName()+"-disConnectClient") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        getCache().close();
        disconnectFromDS();
        JmxClientStatResourcesCleanupDUnitTest.clientConn.destroy();
      }
    });
    logWriter.fine("Exited JmxClientStatResourcesCleanupDUnitTest.stopCache");
  }
  
  /**
   * Stops the cache in the cache VM & disconnects the cache VM from the DS.
   */
  private void stopCache() {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.stopCache");
    Host host    = Host.getHost(0);
    VM   cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-stopCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
      }
    });
    logWriter.fine("Exited JmxClientStatResourcesCleanupDUnitTest.stopCache");
  }
  
  /**
   * Returns the count of all the managed statistic resources under domain 
   * 'GemFire.Statistic'. 
   *  
   * @return Count of the managed statistic resources
   * @throws Exception if JMX operation fails
   */
  private static int getStatResourceCount() throws Exception {
    logWriter.fine("Entered JmxClientStatResourcesCleanupDUnitTest.getStatResourceCount");
    
    String domainString = "GemFire.Statistic:*";
    Set<ObjectName> queryMBeans = mbsc.queryNames(null, new ObjectName(domainString));
    assertNotNull("Query on beans under domain '" + domainString
                  + "' returned null.", queryMBeans);

    logWriter.fine("Exiting JmxClientStatResourcesCleanupDUnitTest.getStatResourceCount");
    return queryMBeans.size();
  }
  
  /**
   * Returns the count of all the managed statistic resources under domain 
   * 'GemFire.Statistic'. 
   *  
   * @return Count of the managed statistic resources
   * @throws Exception if JMX operation fails
   */
  private static int getNotificationCount() throws Exception {
    return notificationCount.get();
  }

  public void setUp() throws Exception {    
    disconnectAllFromDS();
    mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    
    initDSProperties(mcastPort);
    super.setUp();

    // Calls connectToSystem on the JMX agent over JMX.
    // The distributed system MBean ObjectName is collected/cached in a variable
    mbsc = this.agent.getMBeanServer();
    assertNotNull(mbsc);

    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    distributedSystem = (ObjectName)mbsc.invoke(agentName, "connectToSystem",
        new Object[0], new String[0]);
    assertNotNull(distributedSystem);
    notificationCount.set(0);
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();    
    disconnectAllFromDS();
    notificationCount.set(0);
  }
}
