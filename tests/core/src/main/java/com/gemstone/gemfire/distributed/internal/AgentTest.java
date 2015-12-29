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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.admin.jmx.*;
import com.gemstone.gemfire.admin.jmx.internal.*;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache30.*;
import com.gemstone.gemfire.internal.*;

import dunit.*;

import javax.management.*;
import java.io.*;
//import java.net.*;
import java.util.*;

/**
 * This class tests the functionality of the JMX {@link Agent} class.
 */
public class AgentTest
  extends CacheTestCase /*implements AlertListener*/ {

  private Agent agent = null;
  protected MBeanServerConnection mbs = null;
  private DistributionConfig dsCfg = null;
  private static boolean firstTime = true;
  private File agentPropsFile = null;

  public AgentTest(String name) {
    super(name);
  }

//   private volatile Alert lastAlert = null;

//   public void alert(Alert alert) {
//     getLogWriter().info("DEBUG: alert=" + alert);
//     this.lastAlert = alert;
//   }

  public void setUp() throws Exception {
    if (firstTime) {
      disconnectFromDS(); //make sure there's no ldm lying around
      firstTime = false;
    }

    DistributionManager.isDedicatedAdminVM = true;
    boolean done = false;
    try {

    super.setUp();
    populateCache();

    this.dsCfg = new DistributionConfigSnapshot(getSystem().getConfig());

    // Create a gemfire jmx agent in this vm
    this.agentPropsFile = File.createTempFile("agenttest", "properties");
    this.agentPropsFile.deleteOnExit();
    System.setProperty("gfAgentPropertyFile", this.agentPropsFile.getAbsolutePath());

    String httpPort = String.valueOf(
        AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));

    Properties agentProps = new Properties();
    agentProps.setProperty(AgentConfig.HTTP_PORT_NAME, httpPort);
    agentProps.setProperty(AgentConfig.RMI_ENABLED_NAME, "false");
    agentProps.setProperty(DistributedSystemConfig.MCAST_PORT_NAME,
                           String.valueOf(this.dsCfg.getMcastPort()));
    agentProps.setProperty(DistributedSystemConfig.LOCATORS_NAME,
                           this.dsCfg.getLocators());
    agentProps.setProperty(DistributedSystemConfig.LOG_FILE_NAME,
                           this.getUniqueName() + "-agent.log");
    agentProps.setProperty(DistributedSystemConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    agentProps.setProperty(AgentConfig.AUTO_CONNECT_NAME, "false");
    

    this.agent = new AgentImpl(new AgentConfigImpl(agentProps));
    this.agent.start();
    hydra.Log.getLogWriter().info("JMX Agent started successfully...");

    // Find agent modelMBean and configure it
    this.mbs = this.agent.getMBeanServer();
    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    assertTrue(this.mbs.isRegistered(agentName));
    dumpMBean(agentName);
    this.mbs.setAttribute(agentName, new Attribute("mcastPort", new Integer(this.dsCfg.getMcastPort())));
    this.mbs.setAttribute(agentName, new Attribute("mcastAddress",
      hydra.HostHelper.getHostAddress(this.dsCfg.getMcastAddress())));
    this.mbs.setAttribute(agentName, new Attribute("bindAddress", this.dsCfg.getBindAddress()));
    this.mbs.setAttribute(agentName, new Attribute("locators", this.dsCfg.getLocators()));
    {
//      ObjectName dsName = (ObjectName)
        this.mbs.invoke(agentName, "connectToSystem",
                        new Object[]{},
                        new String[]{}
                        );
        hydra.Log.getLogWriter().info("JMX Agent connected to distributed system successfully..."); 
        
        pause(5000);
    }
    done = true;
    } finally {
      if (!done) {
        DistributionManager.isDedicatedAdminVM = false;
      }
    }
  }

  private void checkAttributes(ObjectName n, String[] expected) throws Exception {
    checkAttributes(n, expected, new ArrayList());
  }


  private void checkAttributes(ObjectName n, String[] expected, List attrNullAllowed) throws Exception {
    MBeanInfo info = this.mbs.getMBeanInfo(n);

    if (expected != null) {
      HashSet atts = new HashSet(Arrays.asList(getFeatureNames(info.getAttributes())));
      for (int i=0; i < expected.length; i++) {
        if (!atts.remove(expected[i])) {
          fail("The attribute " + expected[i] + " did not exist on " + n + " (" + atts + ") - make sure it's defined in SystemMemberRegion.java and admin/jmx/mbeans-descriptors.xml");
        }
      }
      if (!atts.isEmpty()) {
        StringBuffer msg = new StringBuffer();
        Iterator it = atts.iterator();
        while (it.hasNext()) {
          msg.append(it.next());
          msg.append(' ');
        }
        fail("Unexpected attributes " + msg.toString() + "on modelMBean " + n + " (" + atts + ")");
      }
    }

    checkAttributeGettors(n, expected, attrNullAllowed);
  }

  private void checkOps(ObjectName n, String[] expected) throws Exception {
    MBeanInfo info = this.mbs.getMBeanInfo(n);
    HashSet ops = new HashSet(Arrays.asList(getFeatureNames(info.getOperations())));
    for (int i=0; i < expected.length; i++) {
      if (!ops.remove(expected[i])) {
        fail("The operation " + expected[i] + " did not exist on " + n);
      }
    }
    if (!ops.isEmpty()) {
      StringBuffer msg = new StringBuffer();
      Iterator it = ops.iterator();
      while (it.hasNext()) {
        msg.append(it.next());
        msg.append(' ');
      }
      fail("Unexpected operations " + msg.toString() + "on modelMBean " + n);
    }
  }
  static private String[] getFeatureNames(MBeanFeatureInfo[] features) {
    String[] result = new String[features.length];
    for (int i=0; i < features.length; i++) {
      result[i] = features[i].getName();
    }
    return result;
  }

  private void dumpMBean(ObjectName n) throws Exception {
    MBeanInfo info = this.mbs.getMBeanInfo(n);
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    pw.println("DUMP: modelMBean info for: " + n);
    pw.println("  className=" + info.getClassName());
    pw.println("  description=\"" + info.getDescription() + "\"");
    {
      MBeanAttributeInfo[] atts = info.getAttributes();
      for (int i=0; i < atts.length; i++) {
        pw.println("    att: " + atts[i].getName()
                           + " type=" + atts[i].getType()
                           + (atts[i].isReadable() ? "" : " UNREADABLE")
                           + (atts[i].isWritable() ? "" : " READONLY")
                           + " desc=\"" + atts[i].getDescription() + "\"");
      }
    }
    {
      MBeanOperationInfo[] ops = info.getOperations();
      for (int i=0; i < ops.length; i++) {
        String impact = "";
        switch (ops[i].getImpact()) {
        case MBeanOperationInfo.ACTION: impact = "ACTION"; break;
        case MBeanOperationInfo.ACTION_INFO: impact = "ACTION/INFO"; break;
        case MBeanOperationInfo.INFO: impact = "INFO"; break;
        case MBeanOperationInfo.UNKNOWN: impact = "UNKNOWN"; break;
        }

        pw.println("    op: " + ops[i].getName()
                           + " impact=" + impact
                           + " returns=" + ops[i].getReturnType()
                           + " desc=\"" + ops[i].getDescription() + "\"");
      }
    }

    pw.flush();
    getLogWriter().info(sw.toString());
  }
//  private void dumpMBeans(String msg) throws Exception {
//    StringWriter sw = new StringWriter();
//    PrintWriter pw = new PrintWriter(sw, true);
//
//    pw.println("DUMP: " + msg);
//      Set names = this.mbs.queryNames(null, null);
//      Iterator i = names.iterator();
//      while (i.hasNext()) {
//        ObjectName on = (ObjectName)i.next();
//        pw.println("DUMP: modelMBean=" + on);
//      }
//  }

  public void tearDown2() throws Exception {
    try {
    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    this.mbs.invoke(agentName, "disconnectFromSystem",
                    new Object[]{},
                    new String[]{}
                    );
    this.mbs.invoke(agentName, "stop",
                    new Object[]{},
                    new String[]{}
                    );
    this.mbs = null;
    System.setProperty("gfAgentPropertyFile", AgentConfig.DEFAULT_PROPERTY_FILE);
    if (this.agentPropsFile != null) {
      this.agentPropsFile.delete();
      this.agentPropsFile = null;
    }
    super.tearDown2();
    assertFalse(this.agent.isConnected());

    // Clean up "admin-only" distribution manager
    disconnectAllFromDS(); //make sure there's no ldm lying around

    } finally {
    DistributionManager.isDedicatedAdminVM = false;
    }
  }


  /**
   * Test {@link com.gemstone.gemfire.admin.jmx.AgentMBean}
   */
  public void testAgent() throws Exception {
    getLogWriter().info("[testAgent] enter");
    this.agent.getLogWriter().info("[testAgent] agent logWriter");

    ObjectName mbeanName = new ObjectName("GemFire:type=Agent");
    assertTrue(this.mbs.isRegistered(mbeanName));
    dumpMBean(mbeanName);
    checkAttributes(mbeanName, new String[]{"autoConnect", "connected", "mcastPort", "mcastAddress", "bindAddress", "locators", "membershipPortRange", "logFile", "logLevel", "logFileSizeLimit", "logDiskSpaceLimit", "propertyFile", "version", "systemId", "sslEnabled", "sslProtocols", "sslCiphers", "sslAuthenticationRequired", "sslProperties"});
    checkOps(mbeanName, new String[]{"getLog", "connectToSystem", "disconnectFromSystem", "stop", "saveProperties", "addSSLProperty", "removeSSLProperty", "manageDistributedSystem"});


    assertEquals(new Integer(this.dsCfg.getMcastPort()), this.mbs.getAttribute(mbeanName, "mcastPort"));
    // setMcastPort is tested in setup
    assertEquals(hydra.HostHelper.getHostAddress(this.dsCfg.getMcastAddress()), this.mbs.getAttribute(mbeanName, "mcastAddress"));
    // setMcastAddress is tested in setup
    assertEquals(this.dsCfg.getBindAddress(), this.mbs.getAttribute(mbeanName, "bindAddress"));
    // setBindAddress is tested in setup
    assertEquals(this.dsCfg.getLocators(), this.mbs.getAttribute(mbeanName, "locators"));
    // setLocators is tested in setup

//     checkAttributeSettor(mbeanName, "autoConnect", Boolean.TRUE);
//     checkAttributeSettor(mbeanName, "logFile", "AgentTest.log");
//     {
//       File f = new File("AgentTest.properties");
//       checkAttributeSettor(mbeanName, "propertyFile", f.getAbsolutePath());
//     }
//     checkAttributeSettor(mbeanName, "logLevel", getGemFireLogLevel());
//     checkAttributeSettor(mbeanName, "logFileSizeLimit", new Integer(10));
//     checkAttributeSettor(mbeanName, "logDiskSpaceLimit", new Integer(20));

    {
//      String log = (String)
      this.mbs.invoke(mbeanName, "getLog",
                                           new Class[0], new String[0]);
      // nyi check log contents
    }

    {
      assertTrue(this.mbs.getAttribute(mbeanName, "version") instanceof String);
    }

    // connectToSystem is tested in setup
    // disconnectFromSystem is tested in teardown
    // stop is tested in teardown

//    {
//      File f = new File("AgentTest.properties");
//      f.delete();
//    }
  }

  private void checkAttributeSettor(ObjectName mbeanName, String attName, Object newValue) throws Exception {
    Object origValue = this.mbs.getAttribute(mbeanName, attName);
    this.mbs.setAttribute(mbeanName, new Attribute(attName, newValue));
    assertEquals(newValue, this.mbs.getAttribute(mbeanName, attName));
    this.mbs.setAttribute(mbeanName, new Attribute(attName, origValue));
  }
  
  /**
   * Test {@link com.gemstone.gemfire.admin.AdminDistributedSystem}
   */
  public void testAdminDistributedSystem() throws Exception {
    ObjectName mbeanName = null;
    {
      Set names = this.mbs.queryNames(null, null);
      Iterator i = names.iterator();
      while (i.hasNext()) {
        ObjectName on = (ObjectName)i.next();
        String v = on.getKeyProperty("type");
        if (v != null && v.equals("AdminDistributedSystem")) {
          if (mbeanName != null) {
            fail("Found more than one AdminDistributedSystem modelMBean");
          } else {
            mbeanName = on;
          }
        }
      }
      if (mbeanName == null) {
        fail("Did not find a AdminDistributedSystem modelMBean");
      }
    }

    assertTrue(this.mbs.isRegistered(mbeanName));
    dumpMBean(mbeanName);
    checkAttributes(mbeanName, new String[]{"id", "systemName", "remoteCommand", "mcastPort", "mcastAddress", "locators", "membershipPortRange", "alertLevel", "refreshInterval", "refreshIntervalForStatAlerts", "canPersistStatAlertDefs", "missingPersistentMembers"});
    checkOps(mbeanName, new String[]{"start", "stop", "waitToBeConnected", "manageCacheServer", "manageCacheServers", "manageCacheVm", "manageCacheVms", "manageSystemMemberApplications", "monitorGemFireHealth", "displayMergedLogs", "getLicense", "getLatestAlert", "manageDistributionLocator", "manageDistributionLocators", "createDistributionLocator", "manageSystemMember", "getAllStatAlertDefinitions", "updateAlertDefinition", "updateAlertDefinitionForMember", "removeAlertDefinition", "isAlertDefinitionCreated", "revokePersistentMember", "shutDownAllMembers"});

    this.mbs.getAttribute(mbeanName, "id");

    checkAttributeSettor(mbeanName, "remoteCommand", "ssh {HOST} {CMD}");

    assertEquals(new Integer(this.dsCfg.getMcastPort()), this.mbs.getAttribute(mbeanName, "mcastPort"));

    //assertEquals(null, this.mbs.getAttribute(mbeanName, "mcastAddress"));
    // KIRK:
    getLogWriter().info("id = " + this.mbs.getAttribute(mbeanName, "id"));
    getLogWriter().info("mcastAddress = " + this.mbs.getAttribute(mbeanName, "mcastAddress"));
    getLogWriter().info("this.dsCfg.getLocators() = " +  this.dsCfg.getLocators());
    //getLogWriter().info("BUG: spider @todo klund this.mbs.getAttribute(mbeanName, \"mcastAddress\")=" + this.mbs.getAttribute(mbeanName, "mcastAddress"));

    assertEquals(hydra.HostHelper.getHostAddress(this.dsCfg.getMcastAddress()), this.mbs.getAttribute(mbeanName, "mcastAddress"));
    assertEquals(this.dsCfg.getLocators(), this.mbs.getAttribute(mbeanName, "locators"));

    // can't test "start" action here
    // can't test "stop" action here

    /* TODO
    try {
      this.mbs.invoke(mbeanName, "createGemFireManager",
                      new Object[]{},
                      new String[]{}
                      );
      fail("expected UnsupportedOperationException");
    } catch (MBeanException ex) {
      if (!(ex.getCause() instanceof UnsupportedOperationException)) {
        fail("expected UnsupportedOperationException but had " + ex.getCause());
      }
    }
    */

    {
      //dumpMBeans("before manageSystemMemberApplications");
      final ObjectName nam = mbeanName;
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          try {
            ObjectName[] appNames;
            appNames = (ObjectName[])mbs.invoke(nam, "manageSystemMemberApplications",
                new Object[]{},
                new String[]{}
                );
            return appNames.length == 4;
          }
          catch (Exception e) {
            fail("unexpected exception", e);
          }
          return false; // NOTREACHED
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
      //dumpMBeans("after manageSystemMemberApplications");
      ObjectName[] appNames = (ObjectName[])
          this.mbs.invoke(mbeanName, "manageSystemMemberApplications",
          new Object[]{},
          new String[]{}
          );
      assertEquals(4, appNames.length);
      for (int i=0; i < appNames.length; i++) {
        checkApp(appNames[i], false);
      }
    }

    {
      //dumpMBeans("before monitorGemFireHealth");
      ObjectName healthName
        = (ObjectName)this.mbs.invoke(mbeanName, "monitorGemFireHealth",
                                         new Object[]{},
                                         new String[]{}
                                         );
      //dumpMBeans("after monitorGemFireHealth");
      assertNotNull("Got null health MBean for AdminDistributedSystem " +
                    mbeanName, healthName);
      checkHealth(healthName);
    }

    {
//      String logs = (String)
      this.mbs.invoke(mbeanName, "displayMergedLogs",
                                            new Object[]{},
                                            new String[]{}
                                            );
      // nyi veriry log String
    }

    {
//      Properties lInfo = (Properties)
        this.mbs.invoke(mbeanName, "getLicense",
                                                     new Object[]{},
                                                     new String[]{}
                                                     );
      // nyi verify Properties
    }

    { // test AdminDistributedSystem alert listening...
      final String oldAlert = (String)this.mbs.invoke(mbeanName, "getLatestAlert",
                                                    new Object[]{},
                                                    new String[]{}
                                                    );
      if (oldAlert.equals("")) {
        // ok
      } else if (oldAlert.indexOf("Could not rename") != -1 || oldAlert.indexOf("resulted in a non-positive timestamp delta") != -1) {
        // ok
      } else {
        assertEquals("", oldAlert);
      }
      String msg = "TESTING A VERY UNIQUE AgentTest MESSAGE " + (new Date());

      /*for (int h = 0; h < Host.getHostCount(); h++) {
        Host host = Host.getHost(h);

        for (int v = 0; v < host.getVMCount(); v++) {
          VM vm = host.getVM(v);
          vm.invoke(populator);
        }
      }*/
      Host host = Host.getHost(0);
      writeLogMessage(msg, host.getVM(0));

//      int sleepCount = 0;
      final ObjectName nam = mbeanName;
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          try {
            String latestAlert = (String)mbs.invoke(nam, "getLatestAlert",
                new Object[]{},
                new String[]{}
                );
            return !latestAlert.equals(oldAlert);
          }
          catch (Exception e) {
            fail("unexpected exception", e);
          }
          return false; // NOTREACHED
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);
      String latestAlert = (String)this.mbs.invoke(mbeanName, "getLatestAlert",
          new Object[]{},
          new String[]{}
          );
      assertTrue("unexpected alert msg: " + latestAlert,
                 latestAlert.indexOf(msg) > -1);
      //assertEquals(managers[m], latestAlert.getGemFireVM());
    }

    // @todo klund - test getLatestAlert
  }

  private void checkApp(ObjectName appName, boolean hasProxy)
    throws Exception {

    //SystemMember
    final ObjectName mbeanName = appName; //getMemberMBeanName(app);

    if (appName.toString().indexOf(SystemMemberType.MANAGER.getName()) < 0) {
    //if (!(app instanceof GemFireManager)) {
      assertTrue("An mbean named \"" + mbeanName + "\" is not registered", this.mbs.isRegistered(mbeanName));
      dumpMBean(mbeanName);
      checkAttributes(mbeanName, getAppAttributes());
      checkOps(mbeanName, getAppOps());
    }

    /*assertEquals(app.getId(), this.mbs.getAttribute(mbeanName, "id"));
    assertEquals(app.getHost(), this.mbs.getAttribute(mbeanName, "host"));
    assertEquals(new Integer(app.getPort()), this.mbs.getAttribute(mbeanName, "port"));
    */

    {
      Object origValue = this.mbs.getAttribute(mbeanName, "refreshInterval");

      checkAttributeSettor(mbeanName, "archive-disk-space-limit", new Integer(1));
      checkAttributeSettor(mbeanName, "archive-file-size-limit", new Integer(1));
      // nyi setting license-file
      // nyi setting license-type
      checkAttributeSettor(mbeanName, "log-disk-space-limit", new Integer(1));
      checkAttributeSettor(mbeanName, "log-file-size-limit", new Integer(1));
      checkAttributeSettor(mbeanName, "log-level", getDUnitLogLevel());
      // nyi setting statistic-archive-file
      checkAttributeSettor(mbeanName, "statistic-sample-rate", new Integer(1001));
      checkAttributeSettor(mbeanName, "statistic-sampling-enabled", Boolean.FALSE);

      //this.mbs.setAttribute(mbeanName, new Attribute("refreshInterval", origValue));
    }

    // RefreshInterval cannot be set
    Exception ex = null;
    try {
      checkAttributeSettor(mbeanName, "refreshInterval", new Integer(0));
    } catch (javax.management.MBeanException mbEx) {
      ex = mbEx;
    }
    assertTrue("RefreshInterval cannot be reset. An Exception is expected", ex!=null); 

    {
//      String log = (String)
      this.mbs.invoke(mbeanName, "getLog",
                                           new Object[]{},
                                           new String[]{}
                                           );
      // nyi log contents
    }
    {
//      Properties lInfo = (Properties)
          this.mbs.invoke(mbeanName, "getLicense",
                                                     new Object[]{},
                                                     new String[]{}
                                                     );
      // nyi verify properties contents
    }
    {
      assertTrue(this.mbs.getAttribute(mbeanName, "version") instanceof String);
    }
    this.mbs.invoke(mbeanName, "refreshConfig",
                    new Object[]{},
                    new String[]{}
                    );
    {
      //com.gemstone.gemfire.admin.StatisticResource
      ObjectName[] statNames = (ObjectName[])
        this.mbs.invoke(mbeanName, "manageStats",
                        new Object[]{},
                        new String[]{}
                        );
      for (int i=0; i < statNames.length; i++) {
        checkStatResource(appName, statNames[i]);
      }
    }
    {
      ObjectName cacheName = (ObjectName)
        this.mbs.invoke(mbeanName, "manageCache",
                        new Object[]{},
                        new String[]{}
                        );
      checkCache(cacheName, hasProxy); //, app.getId());
    }
  }

  /**
   * Checks that the MBean for a <code>GemFireHealth</code> is
   * configured correctly.
   */
  private void checkHealth(ObjectName healthName) throws Exception {
    //GemFireHealth
    final ObjectName mbeanName = healthName;
      //new ObjectName(((ManagedResource) health).getMBeanName());
    assertTrue("An mbean named \"" + mbeanName + "\" is not registered", this.mbs.isRegistered(mbeanName));
    dumpMBean(mbeanName);
    checkAttributes(mbeanName, getHealthAttributes());
    checkOps(mbeanName, getHealthOps());

    checkAttributeGettors(mbeanName, getHealthAttributes());

    assertEquals(GemFireHealth.GOOD_HEALTH,
                 this.mbs.getAttribute(mbeanName, "health"));
  }

  private void checkCache(ObjectName cacheName, boolean hasProxy)
    throws Exception {

    //SystemMemberCache, String parentId
    final ObjectName mbeanName = cacheName;
      /*new ObjectName("GemFire.Cache:name="
                     + MBeanUtil.makeCompliantMBeanNameProperty(cache.getName())
                     + ",id=" + cache.getId()
                     + ",owner=" + MBeanUtil.makeCompliantMBeanNameProperty(parentId)
                     + ",type=Cache");*/
    assertTrue(this.mbs.isRegistered(mbeanName));
    dumpMBean(mbeanName);
    checkAttributes(mbeanName,
                    new String[]{"name", "closed", "lockLease", "lockTimeout",
                                 "rootRegionNames", "searchTimeout", "upTime",
                                 "server",
                                 "updateTime", "netsearchesCompleted",
                                 "netloadsCompleted", "loadTime", "gets",
                                 "netloadTime", "updates", "regions", "entries",
                                 "retries",
                                 "invalidates", "getInitialImagesInProgress",
                                 "destroys", "loadsCompleted",
                                 "loadsInProgress",
                                 "getInitialImageKeysReceived", "netsearchTime",
                                 "getInitialImageTime", "netsearchesInProgress",
                                 "getTime", "putTime",
                                 "getInitialImageTransactionsReceived",
                                 "getInitialImagesCompleted", "puts",
                                 "putallTime", "putalls",
                                 "creates", "eventQueueSize",
                                 "eventQueueThrottleCount",
                                 "eventQueueThrottleTime",
                                 "eventThreads", "misses",
                                 "netloadsInProgress",
                                 "txFailureTime",
                                 "txCommitTime",
                                 "txCommits",
                                 "txRollbackLifeTime",
                                 "txRollbackTime",
                                 "txRollbackChanges",
                                 "txRollbacks",
                                 "txFailureChanges",
                                 "txSuccessLifeTime",
                                 "txFailures",
                                 "txFailedLifeTime",
                                 "txCommitChanges",
                                 "txConflictCheckTime",
                                 "cacheListenerCallsCompleted",
                                 "cacheListenerCallsInProgress",
                                 "cacheListenerCallTime",
                                 "cacheWriterCallsCompleted",
                                 "cacheWriterCallsInProgress",
                                 "cacheWriterCallTime",
                                 "queryExecutionTime",
                                 "queryExecutions",
                                 "queryResultsHashCollisionProbeTime",
                                 "queryResultsHashCollisions",
                                 "partitionedRegionQueryRetries",
                                 "indexUpdateInProgress",
                                 "indexUpdateCompleted",
                                 "indexUpdateTime",
                                   "reliableQueuedOps",
                                   "reliableRegionsQueuing",
                                   "reliableRegionsMissing",
                                   "reliableQueueMax",
                                   "reliableQueueSize",
                                   "reliableRegions",
                                   "reliableRegionsMissingFullAccess",
                                   "reliableRegionsMissingLimitedAccess",
                                   "reliableRegionsMissingNoAccess",
                                   "eventsQueued",
                                   "partitionedRegions",
                                 "clears",
                                 "diskTasksWaiting",
                                 "evictorJobsStarted",
                                 "evictorJobsCompleted",
                                 "evictorQueueSize",
                                 "evictWorkTime",
                                 "nonSingleHopsCount",
                                 "metaDataRefreshCount",
                                 "deltaUpdates",
                                 "deltaUpdatesTime",
                                 "deltaFailedUpdates",
                                 "deltasPrepared",
                                 "deltasPreparedTime",
                                 "deltasSent",
                                 "deltaFullValuesSent",
                                 "deltaFullValuesRequested",
                                 "conflatedEvents",
                                 "tombstones",
                                 "tombstoneGCs",
                                 "replicatedTombstonesSize",
                                 "nonReplicatedTombstonesSize",
                                 "clearTimeouts",
                                 "importedEntries",
                                 "importTime",
                                 "exportedEntries",
                                 "exportTime",
                                 "compressTime",
                                 "compressions",
                                 "preCompressedBytes",
                                 "decompressions",
                                 "postCompressedBytes",
                                 "decompressTime",
                                 "evictByCriteria_evaluations",
                                 "txRemoteRollbackLifeTime",
                                 "txRemoteFailureTime",
                                 "txRemoteRollbackTime",
                                 "txRemoteCommits",
                                 "txRemoteFailedLifeTime",
                                 "evictByCriteria_evaluationTime",
                                 "txRemoteCommitTime",
                                 "evictByCriteria_evictionsInProgress",
                                 "deltaGetInitialImagesCompleted",
                                 "evictByCriteria_evictions",
                                 "evictByCriteria_evictionTime",
                                 "txRemoteSuccessLifeTime",
                                 "txRemoteFailureChanges",
                                 "txRemoteRollbacks",
                                 "txRemoteFailures",
                                 "txRemoteRollbackChanges",
                                 "txRemoteCommitChanges"
                                 });
    List ops = new ArrayList();
    ops.add("refresh");
    ops.add("manageRegion");
    ops.add("getStatistics");
    ops.add("manageBridgeServer");
    ops.add("manageBridgeServers");
    ops.add("manageCacheServer");
    ops.add("manageCacheServers");
    ops.add("getSnapshot");
    ops.add("getRegionSnapshot");

    if (hasProxy) {
      ops.add("startCacheProxy");
      ops.add("stopCacheProxy");
    }
    String[] array = new String[ops.size()];
    ops.toArray(array);

    checkOps(mbeanName, array);


    assertEquals(Boolean.FALSE, this.mbs.getAttribute(mbeanName, "closed"));
    checkAttributeSettor(mbeanName, "lockLease", new Integer(66));
    checkAttributeSettor(mbeanName, "lockTimeout", new Integer(66));
    checkAttributeSettor(mbeanName, "searchTimeout", new Integer(66));

    {
      Integer oldValue = (Integer)this.mbs.getAttribute(mbeanName, "upTime");
      Thread.sleep(2000);
      assertEquals(oldValue, this.mbs.getAttribute(mbeanName, "upTime"));
      this.mbs.invoke(mbeanName, "refresh",
                      new Object[]{},
                      new String[]{}
                      );
      Integer newValue = (Integer)this.mbs.getAttribute(mbeanName, "upTime");
      assertTrue("expected " + newValue + " > " + oldValue, newValue.compareTo(oldValue) > 0);
    }

    {
      Set roots = (Set)this.mbs.getAttribute(mbeanName, "rootRegionNames");
      Iterator it = roots.iterator();
      while (it.hasNext()) {
        String regName = (String)it.next();
        ObjectName regionOName = (ObjectName)
          this.mbs.invoke(mbeanName, "manageRegion",
                          new Object[]{regName},
                          new String[]{"java.lang.String"}
                          );
        checkRegion(regionOName, cacheName);
      }
    }
  }

  private void checkRegion(ObjectName regOName,
                           ObjectName cacheName) throws Exception {
    // SystemMemberRegion and SystemMemberCache, String parentId
    final ObjectName mbeanName = regOName;
      /*new ObjectName("GemFire.Cache:"
                     + "path="
                     + MBeanUtil.makeCompliantMBeanNameProperty(reg.getName())
                     + ",name="
                     + MBeanUtil.makeCompliantMBeanNameProperty(cache.getName())
                     + ",id=" + cache.getId()
                     + ",owner=" + MBeanUtil.makeCompliantMBeanNameProperty(parentId)
                     + ",type=Region");*/
    assertTrue(this.mbs.isRegistered(mbeanName));
    dumpMBean(mbeanName);

    List attrNullAllowed = new ArrayList();
    attrNullAllowed.add("partitionAttributes");

    checkAttributes(mbeanName, new String[]{
        "name", "fullPath", "userAttribute",
        "cacheLoader", "cacheWriter",
        "evictionAttributes", "cacheListener",
        "keyConstraint",
        "regionTimeToLiveTimeLimit",
        "regionTimeToLiveAction",
        "entryTimeToLiveTimeLimit",
        "earlyAck",
        "entryTimeToLiveAction", "regionIdleTimeoutTimeLimit",
        "regionIdleTimeoutAction", "entryIdleTimeoutTimeLimit",
        "entryIdleTimeoutAction", "mirrorType", "scope", "initialCapacity",
        "dataPolicy",
        "loadFactor", "concurrencyLevel", "statisticsEnabled",
        "entryCount", "subregionCount", "lastModifiedTime",
        "lastAccessedTime", "hitCount", "missCount", "hitRatio",
        "subregionNames", "subregionFullPaths", "persistBackup",
        "diskWriteAttributes", "diskDirs", "membershipAttributes",
        "subscriptionAttributes","partitionAttributes",
        "concurrencyChecksEnabled"}, attrNullAllowed);

    checkOps(mbeanName, new String[]{"refresh", "getCacheListeners"});

    this.mbs.invoke(mbeanName, "refresh",
                    new Object[]{},
                    new String[]{}
                    );

    {
      Set subs = (Set)this.mbs.getAttribute(mbeanName, "subregionNames");
      assertEquals(new Integer(subs.size()),
                   this.mbs.getAttribute(mbeanName, "subregionCount"));

      String regName = (String) this.mbs.getAttribute(mbeanName, "name");

      Iterator it = subs.iterator();
      while (it.hasNext()) {
        String subregName = regName + "/" + (String)it.next();
        //SystemMemberRegion subreg = cache.getRegion(subregName);
        ObjectName subRegOName = (ObjectName)
            this.mbs.invoke(cacheName, "manageRegion",
                            new Object[]{ subregName },
                            new String[]{ "java.lang.String" }
                            );
        checkRegion(subRegOName, mbeanName);
      }
    }

    String[] listeners = (String[])this.mbs.invoke(mbeanName, "getCacheListeners",
                                                   new Object[]{},
                                                   new String[]{}
                                                   );
    assertTrue(listeners.length==0);
  }

  private void checkStatResource(ObjectName appName,
                                 ObjectName srName) throws Exception {
    //com.gemstone.gemfire.admin.StatisticResource
    final ObjectName mbeanName = srName;
    /* new ObjectName("GemFire.Statistic:source="
                     + MBeanUtil.makeCompliantMBeanNameProperty(sm.getId())
                     + ",uid=" + sr.getUniqueId()
                     + ",name=" + sr.getName());*/
    assertTrue(this.mbs.isRegistered(mbeanName));
    dumpMBean(mbeanName);
    checkAttributes(mbeanName, null);
    checkOps(mbeanName, new String[]{"refresh", "getStatistics"});

    assertNotNull(this.mbs.getAttribute(mbeanName, "name"));
    assertNotNull(this.mbs.getAttribute(mbeanName, "description"));

    // get the application id and name for comparison to owner...
    String appId = (String)this.mbs.getAttribute(appName, "id");
    assertNotNull(appId);
    String mbrName = (String)this.mbs.getAttribute(appName, "name");
    assertNotNull(mbrName);

    // now validate the owner value...
    String owner = (String)this.mbs.getAttribute(mbeanName, "owner");
    if (!mbrName.equals(owner) && !appId.equals(owner)) {
      fail("Owner should be '" +mbrName+ "' or '" +appId+ "' instead of: " +owner);
    }

    Object origValue = this.mbs.getAttribute(mbeanName, "refreshInterval");
    // RefreshInterval cannot be set
    Exception ex = null;
    try {
      checkAttributeSettor(mbeanName, "refreshInterval", new Integer(0));
    } catch (javax.management.MBeanException mbEx) {
      ex = mbEx;
    }
    assertTrue("RefreshInterval cannot be reset. An Exception is expected", ex!=null); 

    this.mbs.invoke(mbeanName, "refresh",
                    new Object[]{},
                    new String[]{}
                    );
    {
//      Statistic[] stats = (Statistic[])
          this.mbs.invoke(mbeanName, "getStatistics",
                          new Object[]{},
                          new String[]{}
                          );
    }
  }

  private void checkAttributeGettors(ObjectName mbeanName, String[] atts) throws Exception {
    checkAttributeGettors(mbeanName, atts, new ArrayList());
  }

  private void checkAttributeGettors(ObjectName mbeanName, String[] atts, List attrNullAllowed) throws Exception {
    if (atts == null) {
      MBeanInfo info = this.mbs.getMBeanInfo(mbeanName);
      atts = getFeatureNames(info.getAttributes());
    }
    // make sure we can get every attribute
    for (int i=0; i < atts.length; i++) {
      Object res = this.mbs.getAttribute(mbeanName, atts[i]);
      if (!attrNullAllowed.contains(atts[i]) && res == null) {
        fail("getAttribute " + atts[i] + " returned null");
      }
    }
  }

  private String[] getAppAttributes() {
    ArrayList l = new ArrayList(Arrays.asList(new String[] {"hasCache", "id", "version", "host", "refreshInterval", "distributedMember"}));
    l.addAll(Arrays.asList(AbstractDistributionConfig._getAttNames()));
    return (String[])l.toArray(new String[l.size()]);
  }

  /**
   * Returns the attributes of a health MBean
   */
  private String[] getHealthAttributes() {
    return new String[] { "health", "healthStatus" };
  }

  /**
   * Returns the operations of a health MBean
   */
  private String[] getHealthOps() {
    return new String[] { "resetHealth", "getDiagnosis", "manageGemFireHealthConfig" };
  }

  private String[] getAppOps() {
    return new String[] {"getLog", "getLicense", "manageStats", "manageCache", "refreshConfig", "getRoles", "manageStat", "getConfiguration"};
  }
  private String[] getManagerOps() {
    ArrayList l = new ArrayList(Arrays.asList(new String[] {"waitToStart", "waitToStop", "start", "stop", "manageConnectionStats", "manageManagerStats", "manageSharedClassStats", "createProperties", "removeProperties"}));
    l.addAll(Arrays.asList(getAppOps()));
    return (String[])l.toArray(new String[l.size()]);
  }

//  private ObjectName getMemberMBeanName(SystemMember sm) throws Exception {
//    String type = "Application";
//    if (sm instanceof GemFireManager) {
//      type = "GemFireManager";
//    }
//    return new ObjectName("GemFire.Member:id=" + MBeanUtil.makeCompliantMBeanNameProperty(sm.getId()) + ",type=" + type);
//  }

//   public void DEADCODEtestAgent() {
//     assertEquals("expected empty peer array", 0, agent.listPeers().length);
//     int systemCount = 0;
//     for (int h = 0; h < Host.getHostCount(); h++) {
//       Host host = Host.getHost(h);
//       systemCount += host.getSystemCount();
//     }
//     if (systemCount != agent.listManagers().length) {
//       long endTime = System.currentTimeMillis() + 5000; // wait 5 seconds
//       do {
//         try {
//           Thread.sleep(1000);
//         } catch (InterruptedException ignore) {
//         }
//       } while (systemCount != agent.listManagers().length && System.currentTimeMillis() < endTime);
//     }
//     assertEquals(systemCount, agent.listManagers().length);
//     // note that JoinLeaveListener is not tested since it would require
//     // this test to start and stop systems.
//     agent.disconnect();
//     assertTrue("agent should have been disconnected", !agent.isConnected());
//   }

//   public void DEADCODEtestApplications() throws Exception {
//     ApplicationVM[] apps = agent.listApplications();
//     {
//       int retryCount = 10;
//       while (apps.length < 4) {
//         retryCount--;
//         if (retryCount == 0) {
//           fail("Expected 4 apps but only have " + apps.length);
//         }
//         try {
//           Thread.sleep(500);
//         } catch (InterruptedException ignore){}
//         apps = agent.listApplications();
//       }
//       {
//         //for (int a = 0; a < apps.length; a++) {
//         //  getLogWriter().info("DEBUG: testApplications: apps[" + a + "]=" + apps[a]);
//         //}
//       }
//       assertEquals(4, apps.length);
//     }

//     //final Serializable controllerId = getSystem().getDistributionManager().getId(); //can't do this...
//     for (int i=0; i<apps.length; i++) {
//       //if (apps[i].getId().equals(controllerId)) {
//       //  continue; // skip this one; its the locator vm
//       //}
//       InetAddress host = apps[i].getHost();
//       String appHostName = host.getHostName();
//       try {
//         InetAddress appHost = InetAddress.getByName(appHostName);
//         assertEquals(appHost, host);
//       } catch (UnknownHostException ex) {
//         fail("Lookup of address for host " + appHostName
//              + " failed because " + ex);
//       }

//       StatResource[] stats = apps[i].getStats();
//       assertTrue(stats.length > 0);

//       Config conf = apps[i].getConfig();
//       String[] attNames = conf.getAttributeNames();
//       boolean foundIt = false;
//       for (int j=0; j<attNames.length; j++) {
//         if (attNames[j].equals(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME)) {
//           foundIt = true;
//           assertEquals(conf.getAttribute(attNames[j]), "true");
//           break;
//         }
//       }
//       assertTrue(foundIt);

//       {
//         Properties props = apps[i].getLicenseInfo();
//         assertNotNull(props);
//         String value = props.getProperty("signed.properties.signature");
//         assertNotNull(value);
//       }

//       String[] logs = apps[i].getSystemLogs();
//       assertTrue(logs.length > 0);
//       assertTrue(logs[0].length() > 0);

//       {
//         VM vm = findVMForAdminObject(apps[i]);
// //         getLogWriter().info("DEBUG: found VM " + vm +
// //                             " which corresponds to ApplicationVM "
// //                             + apps[i] + " in testApplications");
//         assertNotNull(vm);
//         String lockName = "cdm_testlock" + i;
//         assertTrue(acquireDistLock(vm, lockName));
//         DLockInfo[] locks = apps[i].getDistributedLockInfo();
//         assertTrue(locks.length > 0);
//         boolean foundLock = false;
//         for (int j=0; j<locks.length; j++) {
//           if (locks[j].getLockName().equals(lockName)) {
//             foundLock = true;
//             assertTrue(locks[j].isAcquired());
//           }
//         }
//         assertTrue(foundLock);
//       }

//       Region[] roots = apps[i].getRootRegions();
//       if (roots.length == 0) {
//         getLogWriter().info("DEBUG: testApplications: apps[" + i + "]=" + apps[i] + " did not have a root region");
//       } else {
//         Region root = roots[0];
//         assertNotNull(root);
//         assertEquals("root", root.getName());
//         assertEquals("/root", root.getFullPath());
//         RegionAttributes attributes = root.getAttributes();
//         assertNotNull(attributes);
//         if (attributes.getStatisticsEnabled()) {
//           assertNotNull(root.getStatistics());
//         }
//         Set subregions = root.subregions(false);
//         assertEquals(3, subregions.size());
//         assertEquals(2, root.keys().size());
//         Region.Entry entry = root.getEntry("cacheObj1");
//         assertNotNull(entry);
//         if (attributes.getStatisticsEnabled()) {
//           assertNotNull(entry.getStatistics());
//         }
//         assertTrue(entry.getValue().equals("null"));

//         /// test lightweight inspection;
//         entry = root.getEntry("cacheObj2");
//         assertNotNull(entry);
//         Object val = entry.getValue();
//         assertTrue(val instanceof String);
//         assertTrue(((String)val).indexOf("java.lang.StringBuffer") != -1);

//         /// test physical inspection
//         //getLogWriter().info("DEBUG: Starting test of physical cache value inspection");
//         apps[i].setCacheInspectionMode(GemFireVM.PHYSICAL_CACHE_VALUE);
//         entry = root.getEntry("cacheObj2");
//         assertNotNull(entry);
//         val = entry.getValue();
//         assertTrue(val instanceof EntryValueNode);
//         EntryValueNode node = (EntryValueNode)val;
//         String type = node.getType();
//         assertTrue(type.indexOf("java.lang.StringBuffer") != -1);
//         assertTrue(!node.isPrimitiveOrString());
//         EntryValueNode[] fields = node.getChildren();
//         assertNotNull(fields);
//         assertTrue(fields.length > 0);

//         /// test destruction in the last valid app
//         int lastIdx = apps.length-1;

//         /*if (i == lastIdx ||
//           (i == lastIdx-1 && apps[lastIdx].getId().equals(controllerId))) {*/

//         if (i == lastIdx) {
//           //getLogWriter().info("DEBUG: starting region destroy from admin apis");
//           int expectedSize = subregions.size() - 1;
//           Region r = (Region)subregions.iterator().next();
//           r.destroyRegion();
//           int retryCount = 40;
//           while (subregions.size() > expectedSize) {
//             retryCount--;
//             if (retryCount == 0) {
//               fail("Waited 20 seconds for region " + r.getFullPath() + "to be destroyed.");
//             }
//             try {
//               Thread.sleep(500);
//             } catch (InterruptedException ignore){}
//             subregions = root.subregions(false);
//           }
//         }
//       }
//     }
//   }

//   public void DEADCODEtestManagers() throws Exception {
//     int retryCount = 10;
//     GfManager[] managers = agent.listManagers();
//     while (managers.length < 2) {
//       retryCount--;
//       if (retryCount == 0) {
//         fail("Expected 2 managers but only have " + managers.length);
//       }
//       try {
//         Thread.sleep(500);
//       } catch (InterruptedException ignore){}
//       managers = agent.listManagers();
//     }
// //     {
// //       getLogWriter().info("DEBUG: managers.length=" + managers.length);
// //       for (int m = 0; m < managers.length; m++) {
// //         getLogWriter().info("DEBUG: managers[" + m + "]=" + managers[m]);
// //         ApplicationProcess[] apps = managers[m].listConnectedProcesses();
// //         for (int a = 0; a < apps.length; a++) {
// //           getLogWriter().info("DEBUG: apps[" + a + "]=" + apps[a]);
// //         }
// //       }
// //     }
//     assertEquals(2, managers.length);

//     for (int m = 0; m < managers.length; m++) {
//       assertEquals(agent, managers[m].getManagerAgent());
//       String systemName = ((SystemConfig)managers[m].getConfig()).getName();
//       GemFireSystem system = null;
//       for (int h = 0; h < Host.getHostCount(); h++) {
//         system = Host.getHost(h).systemNamed(systemName);
//         if (system != null) {
//           break;
//         }
//       }
//       if (system == null) {
//         fail("Could not find system named " + systemName);
//       }
//       {
//         String systemHostName = system.getHost().getHostName();
//         try {
//           InetAddress systemHost = InetAddress.getByName(systemHostName);
//           assertEquals(systemHost, managers[m].getHost());
//         } catch (UnknownHostException ex) {
//           fail("Lookup of address for host " + systemHostName
//                + " failed because " + ex);
//         }
//       }
//       {
//         // Drive letters on Windows can have different cases <barf>
//         String expected = system.getSystemDirectory();
//         String actual = ((SystemConfig)managers[m].getConfig()).getSystemDirectory().getPath();
//         assertTrue("Expected: " + expected + ", got: " + actual,
//                    expected.equalsIgnoreCase(actual));
//       }

//       {
//         ApplicationProcess[] apps = managers[m].listConnectedProcesses();
//         assertEquals("expected zero apps for each manager", 0, apps.length);
//       }

//       {
//         Properties props = managers[m].getLicenseInfo();
//         assertNotNull(props);
//         String value = props.getProperty("signed.properties.signature");
//         assertNotNull(value);
//       }

//       {
//         SystemConfig sc = (SystemConfig)managers[m].getConfig();
//         int oldStatisticSampleRate = sc.getStatisticSampleRate();
//         sc.setStatisticSampleRate(100);
//         managers[m].setConfig(sc);
//         assertEquals(100, ((SystemConfig)managers[m].getConfig()).getStatisticSampleRate());
//         sc.setStatisticSampleRate(oldStatisticSampleRate);
//         managers[m].setConfig(sc);
//         String MSG = "TESTING A VERY UNIQUE AgentTest MESSAGE " + (new Date());
//         //assertTrue(managers[m].tailSystemLog().indexOf(MSG) == -1);
//         Alert oldLastAlert = this.lastAlert;
//         writeLogMessage(MSG, system.getVM(0));
//         int sleepCount = 0;
//         do {
//           Thread.sleep(500);
//         } while (this.lastAlert == oldLastAlert && sleepCount++ < 10);
//         assertTrue("expected lastAlert to change: " + this.lastAlert, oldLastAlert != this.lastAlert);
//         assertTrue("unexpect alert msg " + this.lastAlert.getMessage(), this.lastAlert.getMessage().indexOf(MSG) != -1);
//         assertEquals(managers[m], this.lastAlert.getGemFireVM());

//         // For now just make sure we can call tailSystemLog.
//         // On Windows it can return null because the system has not flushed
//         // its log file yet.
//         managers[m].getSystemLogs();
//         //assertTrue(managers[m].tailSystemLog().indexOf(MSG) != -1);

//         {
//           long sharedDateCount = getClassCount(managers[m], "SharedDate");
//           LockListenerImpl ll = new LockListenerImpl();
//           getLogWriter().info("DEBUG: Adding LockListener " + ll + " to manager[" + m + "] " + managers[m]);
//           managers[m].addLockListener(ll);
//           int stuckLockObjId = prepareStickLock(system.getVM(0));
//           AsyncInvocation aiStuck = stickLock(system.getVM(0));
//           int waitCount = 0;
//           getLogWriter().info("DEBUG: stickLock() called, waiting for notification");
//           while (ll.stuckManager == null) {
//             if (waitCount == 5) {
//               fail("Waited 5 seconds for stuck lock to be detected");
//             }
//             Thread.sleep(1000);
//             waitCount++;
//           }
//           managers[m].removeLockListener(ll);
//           LockInfo stuckLock = null;
//           try {
//             for (int i=0; i < ll.stuckLocks.length; i++) {
//               if (stuckLockObjId == ll.stuckLocks[i].getLockObjectId()) {
//                 stuckLock = ll.stuckLocks[i];
//                 break;
//               }
//             }
//             assertTrue(stuckLock != null);
//             assertEquals(stuckLockObjId, stuckLock.getLockObjectId());
//             assertTrue(stuckLock.isAcquired());
//             assertEquals(system.getVM(0).getPid(), stuckLock.getProcessId());
//             assertEquals(ll.stuckManager, managers[m]);
//             assertTrue("expected " + getClassCount(managers[m], "SharedDate") + " > " + sharedDateCount, getClassCount(managers[m], "SharedDate") > sharedDateCount);
//             InspectableObject ns = managers[m].getObjectByID(SmOop.OOP_NAMESPACE);
//             assertNotNull(ns);
//             InspectableAttribute[] contents = ns.getElements();
//             assertTrue(contents.length > 0);
//             InspectableObject stuckLockObj = null;
//             for (int i=0; i<contents.length; i++) {
//               if (contents[i].getID() == stuckLockObjId) {
//                 stuckLockObj = contents[i].getObjectValue();
//               }
//             }
//             assertNotNull(stuckLockObj);
//             //InspectableObject stuckLockObj = managers[m].getObjectByID(stuckLockObjId);
//             assertEquals(stuckLockObjId, stuckLockObj.getID());
//             InspectableClass sharedDateClass = managers[m].getClassByID(stuckLockObj.getClassID());
//             String pidStr = "_" + system.getVM(0).getPid() + "_";
//             assertTrue(stuckLockObj.getLockOwner() + " did not contain pid=" + system.getVM(0).getPid(),
//                       stuckLockObj.getLockOwner().indexOf(pidStr) != -1);
//             assertEquals(2, stuckLockObj.getLockWaiters());
//             assertEquals(0, stuckLockObj.getNotifyWaiters());
//             assertTrue(stuckLockObj.isArchiveable());
//             assertTrue(!stuckLockObj.isArchived());
//             assertTrue(stuckLockObj.setArchived(true));
//             assertTrue(stuckLockObj.isArchived());
//             {
//               StatResource stuckLockSR = managers[m].getAsStatistic(stuckLockObj);
//               assertTrue(stuckLockSR != null);
//               assertEquals((long)stuckLockObj.getID(), stuckLockSR.getResourceID());
//               assertEquals(stuckLockObj.getID(), stuckLockSR.getID());
//               assertEquals(stuckLockObj.getType(), stuckLockSR.getType());
//               assertEquals(1, stuckLockSR.getStats().length);
//               Stat statField = stuckLockSR.getStats()[0];
//               assertEquals("fastTime", statField.getName());
//             }
//             {
//               StatResource[] stats = managers[m].getStats();
//               boolean foundIt = false;
//               for (int i = 0; i < stats.length; i++) {
//                 if (stats[i].getID() == stuckLockObj.getID()) {
//                   foundIt = true;
//                   break;
//                 }
//               }
//               assertTrue("Did not find stat resource with id " + stuckLockObj.getID(),
//                          foundIt);
//             }
//             assertTrue(!stuckLockObj.setArchived(false));
//             assertTrue(!stuckLockObj.isArchived());
//             {
//               StatResource[] stats = managers[m].getStats();
//               boolean foundIt = false;
//               for (int i = 0; i < stats.length; i++) {
//                 if (stats[i].getID() == stuckLockObj.getID()) {
//                   foundIt = true;
//                   break;
//                 }
//               }
//               assertTrue("Found stat resource with id " + stuckLockObj.getID(),
//                          !foundIt);
//             }
//             assertEquals(InspectableObject.DIRECT_SHARED, stuckLockObj.getSharingMode());
//             assertTrue(!stuckLockObj.isArray());
//             assertTrue(!stuckLockObj.isString());
//             assertTrue(!stuckLockObj.hasElements());
//             assertEquals(1, stuckLockObj.getNumFields());
//             InspectableAttribute field = stuckLockObj.getFields()[0];
//             assertEquals("fastTime", field.getName());
//             assertTrue(field.isPrimitive());
//             assertTrue(field.isNumeric());
//             assertTrue(!field.hasChildren());
//             field.getPrimitiveValue();
//             try {
//               field.getObjectValue();
//               fail("Expected RuntimeAdminException");
//             } catch (RuntimeAdminException expected) {
//             }
//             assertEquals("long", field.getType());

//             assertEquals("com.gemstone.gemfire.util.SharedDate", stuckLockObj.getType());
//             assertEquals("com.gemstone.gemfire.util.SharedDate", sharedDateClass.getName());

//             {
//               LockInfo[] allLocks = managers[m].getLockInfo();
//               boolean foundIt = false;
//               for (int l=0; l < allLocks.length; l++) {
//                 if (stuckLockObjId == allLocks[l].getLockObjectId()) {
//                   foundIt = true;
//                 }
//               }
//               if (!foundIt) {
//                 fail("Excepted " + stuckLockObjId + " objectId in lockInfo array.");
//               }
//             }
//           } finally {
//             if (stuckLock != null) {
//               managers[m].clearLock(stuckLock);
//             }
//             aiStuck.join(5000);
//             if (aiStuck.isAlive()) {
//               // the thread is hung. Clean up that vms gemfire connection.
//               disconnectFromGemFire(system.getVM(0));
//             }
//           }
//           assertTrue("stickLock did not complete within 5 seconds", !aiStuck.isAlive());
//           assertTrue("Unexpected exception from stickLock invocation"
//                      + aiStuck.getException(), !aiStuck.exceptionOccurred());
//           {
//             Thread.sleep(1000);
//             LockInfo[] allLocks = managers[m].getLockInfo();
//             for (int l=0; l < allLocks.length; l++) {
//               if (stuckLockObjId == allLocks[l].getLockObjectId()) {
//                 if (allLocks[l].isAcquired()) {
//                   fail("Excepted " + stuckLockObjId + " was still acquired after it was unstuck lockInfo="  + allLocks[l]);
//                 }
//               }
//             }
//           }
//         }
//         {
//           ConnectionListenerImpl cl = new ConnectionListenerImpl();
//           managers[m].addConnectionListener(cl);
//           connectToGemFire(system.getVM(0));
//           long conId = getConId(system.getVM(0));
//           int waitCount = 0;
//           while (!cl.hasJoined(conId)) {
//             Thread.sleep(1000);
//             waitCount++;
//             if (waitCount == 9) {
//               System.err.println("DEBUG: waited 9 seconds for connection " + conId + " to join");
//               disconnectFromGemFire(system.getVM(0));
//               fail("Waited 9 seconds for connection " + conId + " to join");
//             }
//           }

//           disconnectFromGemFire(system.getVM(0));
//           waitCount = 0;
//           while (!cl.hasLeft(conId)) {
//             Thread.sleep(1000);
//             waitCount++;
//             if (waitCount == 5) {
//               fail("Waited 5 seconds for connection " + conId + " to leave");
//             }
//           }
//           managers[m].removeConnectionListener(cl);
//         }
//       }
//     }
//   }

//   public void DEADCODEtestHealthListener() {
//     {
//       int retryCount = 10;
//       GfManager[] managers = agent.listManagers();
//       while (managers.length < 1) {
//         retryCount--;
//         if (retryCount == 0) {
//           fail("Expected at least one manager but only have " + managers.length);
//         }
//         try {
//           Thread.sleep(500);
//         } catch (InterruptedException ignore){}
//         managers = agent.listManagers();
//       }
//       doHealthListener(managers[0]);
//     }
//     {
//       ApplicationVM[] apps = agent.listApplications();
//       int retryCount = 10;
//       while (apps.length < 1) {
//         retryCount--;
//         if (retryCount == 0) {
//           fail("Expected at least one app but only have " + apps.length);
//         }
//         try {
//           Thread.sleep(500);
//         } catch (InterruptedException ignore){}
//         apps = agent.listApplications();
//       }
//       doHealthListener(apps[0]);
//     }
//   }
//   private static void sameObjectArray(Object[] sa1, Object[] sa2) {
//     assertEquals(Arrays.asList(sa1), Arrays.asList(sa2));
//   }
//   private void doHealthListener(GemFireVM vm) {
//     HealthListenerImpl hl = new HealthListenerImpl();
//     // NOTE: this test will only pass if the DummyHealthEvaluator is used
//     try {
//       vm.addHealthListener(hl, new HealthConfigImpl());
//       assertEquals(0, hl.getStatus());
//       sameObjectArray(new String[]{}, vm.getHealthDiagnosis());
//       hl.waitForStatusChange(0);
//       assertEquals(1, hl.getStatus());
//       sameObjectArray(new String[]{"status is ok"}, vm.getHealthDiagnosis());
//       hl.resetStatus();
//       assertEquals(0, hl.getStatus());
//       vm.resetHealthStatus();
//       hl.waitForStatusChange(0);
//       assertEquals(1, hl.getStatus());
//       sameObjectArray(new String[]{"status is ok"}, vm.getHealthDiagnosis());
//       hl.waitForStatusChange(1);
//       assertEquals(2, hl.getStatus());
//       sameObjectArray(new String[]{"status is poor", "be careful"}, vm.getHealthDiagnosis());
//     } finally {
//       vm.removeHealthListener();
//     }
//   }
//   static class HealthConfigImpl implements HealthConfig {
//     // nothing needed
//   }
//   static class HealthListenerImpl implements HealthListener {
//     private int currentStatus = 0;

//     public void healthChanged(int status) {
//       synchronized (this) {
//         if (this.currentStatus != status) {
//           this.currentStatus = status;
//           this.notify();
//         }
//       }
//     }
//     public int getStatus() {
//       synchronized (this) {
//         return this.currentStatus;
//       }
//     }
//     public void resetStatus() {
//       synchronized (this) {
//         this.currentStatus = 0;
//       }
//     }
//     public int waitForStatusChange(int oldStatus) {
//       synchronized (this) {
//         if (this.currentStatus == oldStatus) {
//           try {
//             this.wait(6000);
//             if (this.currentStatus == oldStatus) {
//               fail("waitForStatusChange did not see status change from " + oldStatus);
//             }
//           } catch (InterruptedException ignore) {
//           }
//         }
//         return this.currentStatus;
//       }
//     }
//   }

  private void populateCache() {

    AttributesFactory fact = new AttributesFactory();
    fact.setScope(Scope.DISTRIBUTED_NO_ACK);
    final RegionAttributes rAttr = fact.create();

    final SerializableRunnable populator = new SerializableRunnable(){
        public void run() {
          try {
            createRegion("cdm-testSubRegion1", rAttr);
            createRegion("cdm-testSubRegion2", rAttr);
            createRegion("cdm-testSubRegion3", rAttr);
            remoteCreateEntry("", "cacheObj1", null);
            StringBuffer  val = new StringBuffer("userDefValue1");
            remoteCreateEntry("", "cacheObj2", val);
          } catch (CacheException ce) {
            fail("Exception while populating cache:\n" + ce);
          }
        }
    };

    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(populator);
      }
    }
  }



  /**
   * Puts (or creates) a value in a region named <code>regionName</code>
   * named <code>entryName</code>.
   */
  protected void remoteCreateEntry(String regionName,
                                          String entryName,
                                          Object value)
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    region.create(entryName, value);


    getLogWriter().info("Put value " + value + " in entry " +
                        entryName + " in region '" +
                        region.getFullPath() +"'");

  }

  // ------------------------------------------------------------------------

//   private void connectToGemFire(VM vm) {
//     vm.invoke(DistributedTestCase.class, "remoteConnectToGemFire",
//               new Object[] { vm.getSystem().getSystemDirectory() });
//   }
//   private void disconnectFromGemFire(VM vm) {
//     vm.invoke(DistributedTestCase.class, "remoteDisconnectFromGemFire");
//   }
//  private long getConId(VM vm) {
//      return vm.invokeLong(this.getClass(), "remoteGetConId");
//  }
//  private static long remoteGetConId() {
//    return GemFireConnectionFactory.getInstance().getId();
//  }

  /**
   * Accessed via reflection.  DO NOt REMOVE
   */
  protected static void remoteWriteLogMessage(String msg) {
    InternalDistributedSystem.getAnyInstance().getLogWriter().severe(msg);
  }
  private void writeLogMessage(String msg, VM vm) {
    vm.invoke(this.getClass(), "remoteWriteLogMessage",
              new Object[] { msg });
  }

//   private long getClassCount(GfManager mgr, String className) {
//     InspectableClass[] classes = mgr.getSharedMemoryContents();
//     for (int i=0; i < classes.length; i++) {
//       if (classes[i].getName().endsWith(className)) {
//         return classes[i].getNumInstances();
//       }
//     }
//     return 0;
//   }

//   private void connectToGemFire(VM vm) {
//     vm.invoke(DistributedTestCase.class, "remoteConnectToGemFire",
//               new Object[] { vm.getSystem().getSystemDirectory() });
//   }
//   private void disconnectFromGemFire(VM vm) {
//     vm.invoke(DistributedTestCase.class, "remoteDisconnectFromGemFire");
//   }
//   private long getConId(VM vm) {
//       return vm.invokeLong(this.getClass(), "remoteGetConId");
//   }
//   private static long remoteGetConId() {
//     return GemFireConnectionFactory.getInstance().getId();
//   }

//   private static void remoteWriteLogMessage(String msg) {
//     GemFireConnectionFactory.getInstance().getLogWriter().severe(msg);
//   }
//   private void writeLogMessage(String msg, VM vm) {
//     connectToGemFire(vm);
//     try {
//       vm.invoke(this.getClass(), "remoteWriteLogMessage",
//                 new Object[] { msg });
//     } finally {
//       disconnectFromGemFire(vm);
//     }
//   }
//   private static final String stuckName = "AgentTest-stuckLock";
//   private int prepareStickLock(VM vm) {
//     getLogWriter().info("DEBUG: preparing to stick lock in vm " + vm);
//     connectToGemFire(vm);
//     try {
//       return vm.invokeInt(this.getClass(), "remotePrepareStickLock");
//     } finally {
//       disconnectFromGemFire(vm);
//     }
//   }
//   private static int remotePrepareStickLock() {
//     Namespace ns = GemFireConnectionFactory.getInstance().getNamespace();
//     Object o = new SharedDate();
//     ns.put(stuckName, o);
//     LockService.acquire(o);
//     return SharedMemoryHelper.getIdForObject(o);
//   }
//   private AsyncInvocation stickLock(VM vm) {
//     getLogWriter().info("DEBUG: sticking lock in vm " + vm);
//     connectToGemFire(vm);
//     return vm.invokeAsync(this.getClass(), "remoteStickLock");
//   }
//   private static void remoteStickLock() {
//     try {
//       Namespace ns = GemFireConnectionFactory.getInstance().getNamespace();
//       Object o = ns.get(stuckName);
//       LockService.acquire(o); // hangs until stuck lock cleared
//       LockService.release(o);
//       ns.remove(stuckName);
//     } finally {
//       GemFireConnectionFactory.getInstance().close();
//     }
//   }

//   private boolean acquireDistLock(VM vm, String lockName) {
//     return vm.invokeBoolean(this.getClass(), "remoteAcquireDistLock",
//                             new Object[]{lockName});
//   }

//   private static boolean remoteAcquireDistLock(String lockName) {
//     String serviceName = "cdmtest_service";
//     DistributedLockService service =
//       DistributedLockService.getServiceNamed(serviceName);
//     if (service == null) {
//       service = DistributedLockService.
//         create(serviceName, InternalDistributedSystem.getAnyInstance());
//     }
//     assertNotNull(service);
//     try {
//       return service.lock(lockName, 1000, 3000);
//     } catch (Exception e) {
//       return false;
//     }
//   }

//   private VM findVMForAdminObject(GemFireVM adminObj) {
//     for (int i=0; i<Host.getHostCount(); i++) {
//       Host host = Host.getHost(i);
//       for (int j=0; j<host.getVMCount(); j++) {
//         VM vm = host.getVM(j);
//         Serializable id = getJavaGroupsIdForVM(vm);
//         if (adminObj.getId().equals(id)) {
//           return vm;
//         }
//       }
//     }
//     return null;
//   }

//   private Serializable getJavaGroupsIdForVM(VM vm) {
//     return (Serializable)vm.invoke(this.getClass(), "remoteGetJavaGroupsIdForVM");
//   }
//   private static Serializable remoteGetJavaGroupsIdForVM() {
//     InternalDistributedSystem sys = (InternalDistributedSystem)
//       InternalDistributedSystem.getAnyInstance();
//     return sys.getDistributionManager().getDistributionManagerId();

//   }


//   static class LockListenerImpl implements LockListener {
//     public volatile GfManager stuckManager = null;
//     public volatile LockInfo[] stuckLocks = null;
//     public void stuckLock(GfManager source, LockInfo[] locks) {
//       stuckManager = source;
//       stuckLocks = locks;
//       getLogWriter().info("DEBUG: LockListenerImpl.stuckManager set to " + stuckManager +
//                            " in LockListener " + this);
//     }
//     public void deadLock(GfManager source, LockInfo[] locks) {
//       // nyi
//     }
//   }
//   static class ConnectionListenerImpl implements ConnectionListener {
//     public Map joinedMap = new HashMap();
//     public Map leftMap = new HashMap();
//     public void connectionJoined(GfManager source, ApplicationProcess joined) {
//       getLogWriter().info("DEBUG: connectionJoined " + (new Date()) +
//                            " source=" + source + ", joined=" + joined);
//       joinedMap.put(new Long(joined.getId()), joined);
//     }
//     public void connectionLeft(GfManager source, ApplicationProcess left) {
//       getLogWriter().info("DEBUG: connectionLeft " + (new Date()) +
//                            " source=" + source + ", left=" + left);
//       leftMap.put(new Long(left.getId()), left);
//     }
//     public boolean hasJoined(long id) {
//       return joinedMap.get(new Long(id)) != null;
//     }
//     public boolean hasLeft(long id) {
//       return leftMap.get(new Long(id)) != null;
//     }
//     public void cacheCreated(GfManager source, ApplicationProcess app){}
//     public void cacheClosed(GfManager source, ApplicationProcess app){}
//   }
}
