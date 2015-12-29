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

package management.test.cli;

import static management.test.cli.ResultValidatorHelper.*;
import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.ClientDescription;
import hydra.ClientPrms;
import hydra.ClientVmInfo;
import hydra.DistributedSystemBlackboard;
import hydra.DistributedSystemHelper;
import hydra.GemFireDescription;
import hydra.GsRandom;
import hydra.HostHelper;
import hydra.JMXManagerBlackboard;
import hydra.JMXManagerHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.PortBlackboard;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedLock;
import hydra.blackboard.SharedMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import management.cli.TestableGfsh;
import management.test.federation.FederationBlackboard;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import parReg.ParRegUtil;
import pdx.PdxTest;
import recovDelay.PrState;
import util.ClassBuilder;
import util.DeclarativeGenerator;
import util.NameFactory;
import util.RandomValues;
import util.SilenceListener;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.MembershipAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

/**
 * @author lynn
 *
 */
public class CommandTest {

  private static final String VERSION = "v7.5.Beta";

  private static final String CMD_ALTER_DISKSTORE     = "alter disk-store";
  private static final String CMD_ALTER_REGION        = "alter region";
  private static final String CMD_ALTER_RUNTIME       = "alter runtime";
  private static final String CMD_CREATE_ASYNC_EVENT_QUEUE = "create async-event-queue";
  private static final String CMD_CREATE_DISK_STORE   = "create disk-store";
  private static final String CMD_CREATE_INDEX        = "create index";
  private static final String CMD_CLOSE_DURABLE_CQ    = "close durable-cq";
  private static final String CMD_CLOSE_DURABLE_CLIENT = "close durable-client";
  private static final String CMD_DEPLOY              = "deploy";
  private static final String CMD_DESCRIBE_CONFIG     = "describe config";
  private static final String CMD_DESCRIBE_MEMBER     = "describe member";
  private static final String CMD_DESCRIBE_REGION     = "describe region";
  private static final String CMD_DESCRIBE_DISKSTORE  = "describe disk-store";
  private static final String CMD_DESCRIBE_CONNECTION = "describe connection";
  private static final String CMD_DESTROY_INDEX       = "destroy index";
  private static final String CMD_DESTROY_REGION      = "destroy region";
  private static final String CMD_ENCRYPT_PASSWORD    = "encrypt password";
  private static final String CMD_EXPORT_CONFIG       = "export config";
  private static final String CMD_EXPORT_STACKTRACES  = "export stack-traces";
  private static final String CMD_GET                 = "get";
  private static final String CMD_LIST_ASYNC_EVENT_QUEUE = "list async-event-queues";
  private static final String CMD_LIST_DURABLE_CQS    = "list durable-cqs";
  private static final String CMD_LIST_FUNCTIONS      = "list functions";
  private static final String CMD_LIST_MEMBERS        = "list members";
  private static final String CMD_LIST_REGIONS        = "list regions";
  private static final String CMD_LIST_DISKSTORES     = "list disk-stores";
  private static final String CMD_LIST_INDEXES        = "list indexes";
  
  private static final String CMD_LOCATE_ENTRY        = "locate entry";
  private static final String CMD_PUT                 = "put";
  private static final String CMD_REMOVE              = "remove";
  private static final String CMD_SHOW_DEADLOCKS      = "show dead-locks";
  private static final String CMD_SHOW_METRICS        = "show metrics";
  private static final String CMD_SHOW_SUBSCRIPTION_QUEUE_SIZE = "show subscription-queue-size";
  private static final String CMD_SLEEP               = "sleep";
  private static final String CMD_STATUS_LOCATOR      = "status locator";
  private static final String CMD_STATUS_SERVER       = "status server";
  private static final String CMD_UNDEPLOY            = "undeploy";
  private static final String CMD_VERSION             = "version";

  private static final String PARM_PREFIX     = " --";
  private static final String PARM_CATEGORIES = "categories";
  private static final String PARM_DIR        = "dir";
  private static final String PARM_DISK_DIRS  = "disk-dirs";
  private static final String PARM_EXPRESSION = "expression";
  private static final String PARM_FILE       = "file";
  private static final String PARM_GROUP      = "group";
  private static final String PARM_HIDE_DEFAULTS = "hide-defaults";
  private static final String PARM_HOST       = "host";
  private static final String PARM_KEY        = "key";
  private static final String PARM_MATCHES    = "matches";
  private static final String PARM_MEMBER     = "member";
  private static final String PARM_NAME       = "name";
  private static final String PARM_PID        = "pid";
  private static final String PARM_PORT       = "port";
  private static final String PARM_REGION     = "region";
  private static final String PARM_WITH_STATS = "with-stats";
  private static final String PARM_TYPE       = "type";
  private static final String PARM_VALUE      = "value";
  private static final String PARM_DURABLE_CLIENT_ID = "durable-client-id";
  private static final String PARM_ID         = "id";
  private static final String PARM_LISTENER   = "listener";
  private static final String PARM_DURABLE_CQ_NAME   = "durable-cq-name"; 
  private static final String PARM_BATCH_SIZE = "batch-size";
  private static final String PARM_PERSISTENT = "persistent";
  private static final String PARM_DISK_STORE = "disk-store";
  private static final String PARM_MAXIMUM_QUEUE_MEMORY = "max-queue-memory";
  private static final String PARM_LISTENER_PARAM_AND_VALUE = "listener-param";
  private static final String PARM_ALLOW_FORCE_COMPACTION = "allow-force-compaction";
  private static final String PARM_AUTO_COMPACT = "auto-compact";
  private static final String PARM_COMPACTION_THRESHOLD = "compaction-threshold";
  private static final String PARM_MAX_OPLOG_SIZE = "max-oplog-size";
  private static final String PARM_QUEUE_SIZE = "queue-size";
  private static final String PARM_TIME_INTERVAL = "time-interval";
  private static final String PARM_WRITE_BUFFER_SIZE = "write-buffer-size";
  private static final String PARM_VALUE_SEP = "=";
  private static final String PARM_SPACE = " ";
  

  private static final String GROUP_ADMIN   = "adminGroup";
  private static final String GROUP_LOCATOR = "locatorGroup";
  private static final String GROUP_MEMBER  = "memberGroup";

  private static final String UNCOMP_REGION_NAME    = "unCompressedDataTestRegion";
  private static final String COMP_REGION_NAME      = "compressedDataTestRegion";
  private static final String COMPRESSION_PUT_VALUE = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam volutpat lacinia mauris ut fringilla. Maecenas nec sodales neque. In et diam a enim facilisis blandit. Maecenas cursus luctus arcu eget congue. Nunc mi ligula, porttitor molestie aliquet ac, cursus eget turpis. Mauris id porta lacus. Suspendisse adipiscing turpis vitae neque euismod at tristique neque luctus. In pulvinar malesuada rutrum.";

  /** instance of this test class */
  public static CommandTest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;
  public TestableGfsh shell = null;
  private int shellManagerMemberVmId= -1; // the vmId of the manager the shell is connected to

  private static PrintWriter commandOutputFile = null;
  private static JMXConnectorServer connectorService = null;
  private static List<String> definedGroups = null; // groups defined in all members in the test

  // BB keys
  private static final String xmlFileNameKeyPrefix = "xmlForVmId_";
  private static final String memberNameKeyPrefix = "nameForVmId_";
  private static final String groupsKeyPrefix = "groupsForVmId_";
  private static final String memberIdKeyPrefix = "memberIdForVmId_";
  private static final String memberKeyPrefix = "memberForVmId_";
  private static final String regionNamesKey = "regionNames";
  private static final String regionAttrKey = "regionAttrs";
  private static final String joinDSOnlyKey = "connectDsOnlyInVmId_";
  private static final String createCacheOnlyKey = "createCacheOnlyInVmId_";
  private static final String locatorVmIdsKey = "locatorVmIds";
  private static final String jmxManagerVmIdsKey = "jmxManagerVmIds";
  private static final String isManagerKeyPrefix = "isManagerVmId_";
  private static final String expectManagerKeyPrefix = "expectManagerVmId_";
  private static final String memberWithCacheMemberIdKeyPrefix = "memberWithCacheMemberIdForVmId_";
  private static final String jmxManagerSettingKeyPrefix = "jmxManagerForVmId_";
  private static final String cmdErrorKey  = "cmdError";
  private static final String dataErrorKey = "dataError";
  private static final String memberDescriptionKey = "memberDescription";
  private static final String helpErrorKey = "helpError";
  private static final String asyncEventQueueIdKey = "asyncEventQueueIds";



  //=================================================
  // initialization methods

  public synchronized static void HydraTask_createLocator() throws Throwable {
    hydra.DistributedSystemHelper.createLocator();
    CommandBB.getBB().getSharedMap().remove(locatorVmIdsKey);
  }

  public synchronized static void HydraTask_startLocatorAndDS() throws Throwable {
    hydra.DistributedSystemHelper.startLocatorAndAdminDS();

    // write information to the blackboard to be used later so gfsh can connect to it
    SharedLock lock = CommandBB.getBB().getSharedLock();
    lock.lock();
    try {
      Map<Integer, String> locatorVmIdMap = (Map<Integer, String>) CommandBB.getBB().getSharedMap().get(locatorVmIdsKey);
      if (locatorVmIdMap == null) {
        locatorVmIdMap = new HashMap();
      }
      locatorVmIdMap.put(RemoteTestModule.getMyVmid(), RemoteTestModule.getMyHost());
      CommandBB.getBB().getSharedMap().put(locatorVmIdsKey, locatorVmIdMap);
    } finally {
      lock.unlock();
    }
    writeExpectedJmxManagerToBB();
    CommandBB.getBB().getSharedMap().put(memberWithCacheMemberIdKeyPrefix + RemoteTestModule.getMyVmid(), DistributedSystemHelper.getDistributedSystem().getDistributedMember());
  }

  /** Initialize this member based on the hydra config params numMembersJoinDSOnly
   *  and numMembersCreateCacheOnly.
   */
  public synchronized static void HydraTask_configurableInit() {
    int numMembersJoinDSOnly = CommandPrms.getNumMembersJoinDSOnly();
    int numMembersCreateCacheOnly = CommandPrms.getNumMembersCreateCacheOnly();
    long configurableInitCounter = CommandBB.getBB().getSharedCounters().incrementAndRead(CommandBB.configurableInitCounter);
    SharedMap sm = CommandBB.getBB().getSharedMap();
    if ((sm.containsKey(joinDSOnlyKey + RemoteTestModule.getMyVmid())) ||
        (numMembersJoinDSOnly > 0) && (configurableInitCounter <= numMembersJoinDSOnly)) {
      HydraTask_initializeDS();
      Log.getLogWriter().info("Joined distributed system only (no cache creation or regions)");
      CommandBB.getBB().getSharedMap().put(joinDSOnlyKey + RemoteTestModule.getMyVmid(), true);
    } else if ((sm.containsKey(createCacheOnlyKey + RemoteTestModule.getMyVmid())) ||
               (numMembersCreateCacheOnly > 0) && (configurableInitCounter <= (numMembersJoinDSOnly + numMembersCreateCacheOnly))) {
      HydraTask_initializeDS();
      HydraTask_initializeCache();
      Log.getLogWriter().info("Joined distributed system and created cache only (no regions)");
      CommandBB.getBB().getSharedMap().put(createCacheOnlyKey + RemoteTestModule.getMyVmid(), true);
    } else {
      HydraTask_initializeDS();
      HydraTask_initializeCache();
      HydraTask_initializeRegions();
      Log.getLogWriter().info("Joined distributed system, created cache and regions");
    }
  }

  public synchronized static void HydraTask_initializeDS() {
    if (testInstance == null) {
      DistributedSystemHelper.connect();
      testInstance = new CommandTest();
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }

  /** Creates and initializes a server or peer.
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new CommandTest();
      HydraTask_initializeCache();
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }

  public synchronized static void HydraTask_initializeCache() {
    if (CacheHelper.getCache() == null) {
      CacheHelper.createCache("cache1");
      CommandBB.getBB().getSharedMap().put(memberWithCacheMemberIdKeyPrefix + RemoteTestModule.getMyVmid(), DistributedSystemHelper.getDistributedSystem().getDistributedMember());

      writeExpectedJmxManagerToBB();
    }
  }

  /** Using jmxManager properties set with hydra params, determine if this member
   *  is expected to be a manager and write it to the blackbaord.
   */
  private static void writeExpectedJmxManagerToBB() {
    // write expected jmxManager to the blackboard
    String clientName = System.getProperty("clientName");
    ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
    GemFireDescription gfd = cd.getGemFireDescription();
    Boolean jmxManager = gfd.getJMXManager();
    Boolean jmxManagerStart = gfd.getJMXManagerStart();
    if ((jmxManager != null) && (jmxManager) && (jmxManagerStart != null) && (jmxManagerStart)) {
      CommandBB.getBB().getSharedMap().put(expectManagerKeyPrefix + RemoteTestModule.getMyVmid(), new Boolean(true));
    }
  }

  /** Write this members jmxManager value to the blackboard
   *
   */
  public static void HydraTask_writeJmxManagerSettingToBB() {
    String clientName = System.getProperty("clientName");
    ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
    GemFireDescription gfd = cd.getGemFireDescription();
    Boolean jmxManager = gfd.getJMXManager();
    if (jmxManager != null) {
      CommandBB.getBB().getSharedMap().put(jmxManagerSettingKeyPrefix + RemoteTestModule.getMyVmid(), jmxManager);
    }
  }

  /**
   * Creates and initializes a server or peer.
   */
  public synchronized static void HydraTask_initializeRegions() {
    if (CacheHelper.getCache().rootRegions().size() == 0) {
      CommandTestVersionHelper.createRegions();
    }
  }

  /**
   * Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new CommandTest();
      HydraTask_initializeCache();
      CommandTestVersionHelper.createRegions();
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        registerInterest();
      }
    }
  }

  /**
   * Create accessor regions based on the datastore regions already created and
   *  whose datastore attributes are written to the blackboard.
   */
  public synchronized static void HydraTask_multiRegionAccessorInit() {
    if (testInstance == null) {
      testInstance = new CommandTest();
      Cache theCache = CacheHelper.createCache("cache1");
      Map<String, RegionAttributes> regionMap = (Map)(CommandBB.getBB().getSharedMap().get(regionAttrKey));
      for (String regName: regionMap.keySet()) {
        String[] tokens = regName.split("/");
        String totalPath = "";
        for (String regPathToken: tokens) {
          if (regPathToken.length() == 0) {
            continue; // if regName begins with "/" then the first token is the empty string; skip it
          }
          totalPath = totalPath + "/" + regPathToken;
          Region aRegion = theCache.getRegion(totalPath);
          Log.getLogWriter().info("totalPath is " + totalPath + " aRegion is " + aRegion);
          if (aRegion == null) { // not already created so create it now
            RegionAttributes attr = regionMap.get(totalPath);
            if (attr == null) {
              throw new TestException("Test problem: attr is null for region " + totalPath);
            }
            AttributesFactory fac = new AttributesFactory(attr);
            DataPolicy dp = attr.getDataPolicy();
            if (dp.withReplication()) {
              fac.setDataPolicy(DataPolicy.EMPTY);
              fac.setEvictionAttributes(null);
            } else if (dp.withPartitioning()) {
              PartitionAttributesFactory prFac = new PartitionAttributesFactory(attr.getPartitionAttributes());
              prFac.setLocalMaxMemory(0);
              fac.setPartitionAttributes(prFac.create());
            }
            fac.setDiskStoreName(null);

            // now create the accessor region
            int lastIndex = totalPath.lastIndexOf("/");
            String regionCreationName = totalPath.substring(lastIndex+1); // get the end of the path
            if (lastIndex > 0) { // creating a subregion
              String parentPath = totalPath.substring(0, lastIndex);
              Log.getLogWriter().info("Parent path is " + parentPath);
              Region parent = theCache.getRegion(parentPath);
              Log.getLogWriter().info("Creating subregion " + regionCreationName + " of " + parent.getFullPath());
              parent.createSubregion(regionCreationName, fac.create());
              Log.getLogWriter().info("Done creating subregion " + regionCreationName + " of " + parent.getFullPath());
            } else { // creating a root region
              Log.getLogWriter().info("Creating root region " + regionCreationName);
              theCache.createRegion(regionCreationName, fac.create());
              Log.getLogWriter().info("Done creating root region " + regionCreationName);
            }
          }
        }
      }
    }
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  /**
   *  Initialize an instance of this test class, called once per vm.
   */
  private void initializeInstance() {
    if (CommandPrms.getCreateProxyRegions() && CommandPrms.getCreateClientRegions()) {
      throw new TestException("Both CommandPrms.createProxyRegions and CommandPrms.createClientRegions are true");
    }
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
    Log.getLogWriter().info("isBridgeConfiguration is " + isBridgeConfiguration);
    CommandTestVersionHelper.saveMemberMbeanInBlackboard();
    initBlackboard();
  }

  /** Initialize the blackboard
   *
   */
  private static void initBlackboard() {
    // write the member's name, group and distributed member id to the blackboard
    int myVmId = RemoteTestModule.getMyVmid();
    DistributedMember member = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
    SharedMap sharedMap = CommandBB.getBB().getSharedMap();
    sharedMap.put(memberNameKeyPrefix + myVmId, CommandTestVersionHelper.getName(member));
    sharedMap.put(groupsKeyPrefix + myVmId, CommandTestVersionHelper.getGroups(member));
    sharedMap.put(memberIdKeyPrefix + myVmId, member.getId());
    sharedMap.put(memberKeyPrefix + myVmId, member);
  }

 
  /** Create an xml file for the current cache and all its regions and write it
   *  to the blackboard for later use.
   *
   */
  public synchronized static void HydraTask_createXmlFile() {
    if (CacheHelper.getCache() == null) {
      return;
    }

    // create the xml file
    String fileName = getXmlFileName(RemoteTestModule.getMyVmid());
    File aFile = new File(fileName);
    if (aFile.exists()) {
      return;
    }
    DeclarativeGenerator.createDeclarativeXml(fileName, CacheHelper.getCache(), false, true);

    // write the xml file name to the blackboard"
    CommandBB.getBB().getSharedMap().put(xmlFileNameKeyPrefix + RemoteTestModule.getMyVmid(), aFile.getAbsolutePath());
  }

  /** Given a vmId, return the xml file name for it
   *
   * @param vmId The vmId of a test jvm.
   * @return The xml file name (not the complete path) of the xml file for the vmId.
   */
  protected static String getXmlFileName(int vmId) {
    final String xmlFileNamePrefix = "vmId_";
    String fileName = xmlFileNamePrefix + vmId + ".xml";
    return fileName;
  }

  /** Initialize the test instance shell variable. If already initialized then
   *  this is a noop.
   */
  public synchronized void initShell() {
    if (shell == null) {
      String argss[] = {};
      SharedLock lock = CommandBB.getBB().getSharedLock();
      try {
    	lock.lock(); //to prevent simultaneous jline.dll loads caused by multiple shells
        Gfsh.SUPPORT_MUTLIPLESHELL  = true;
        String shellId = Thread.currentThread().getName();
        shell =  new TestableGfsh(shellId, true, argss);
        
        shell.setEnvProperty(Gfsh.ENV_APP_QUIET_EXECUTION, "true");

        int commandWaitSec = CommandPrms.getCommandWaitSec();
        Log.getLogWriter().info("Setting testable shell timeout to " + commandWaitSec + " seconds");
        shell.setTimeout(commandWaitSec);
        shell.start();
        Log.getLogWriter().info("Started testable shell: " + shell);
        startCommandOutputFile();
      } catch (ClassNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IOException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } finally {
    	lock.unlock();
      }
    }
  }

  /**
   * Create a file to write the output of commands to. DO NOT use a LogWriter
   * because that will massage the output before logging. We want this file
   * to contain the raw format of output to visually scan it for problems.
   *
   */
  private synchronized void startCommandOutputFile() {
    if (commandOutputFile == null) {
      final String fileName = "commandOutput_" + RemoteTestModule.getMyPid();
      try {
        commandOutputFile = new PrintWriter(new FileOutputStream(new File(fileName)));
      } catch (FileNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }

  /** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
   *  which is equivalent to a full GII.
   */
  protected static void registerInterest() {
    Set<Region<?,?>> rootRegions = CacheHelper.getCache().rootRegions();
    for (Region aRegion: rootRegions) {
      Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES for region " +
                              aRegion.getFullPath());
      aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
      Log.getLogWriter().info("Done calling registerInterest for all keys, " +
                              "result interest policy KEYS_VALUES, " + aRegion.getFullPath() +
                              " size is " + aRegion.size());
    }
  }

  /** Method to handle starting of managers according to the setting of
   *  CommandPrms-nbrOfManagers.
   *
   *  If CommandPrms-nbrOfManagers <= 0 
   *     return without action; the test writer has the option of making
   *     this member a manager with properties
   *  else
   *     start a manager if the system does not yet have the required
   *     number of managers (specified with CommandPrms-nbrOfManagers).
   */
  public static synchronized void HydraTask_startManagerIfRequired() {
    startManagerIfRequired();
  }

  /** Verify manager status for this member for a test that is configured
   *  such that the product automatically creates manager.
   *
   */
  public static void HydraTask_verifyAutoManagers() {
    Log.getLogWriter().info("In HydraTask_verifyAutoManagers");
    boolean thisMemberIsManager = false;
    boolean isManager = false;
    Cache theCache = CacheHelper.getCache();
    if (theCache != null) {
      ManagementService service = ManagementService.getManagementService(theCache);
      isManager = service.isManager();
      String clientName = System.getProperty("clientName");
      ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
      GemFireDescription gfd = cd.getGemFireDescription();
      Boolean jmxManager = gfd.getJMXManager();
      boolean thisMemberIsEligible = false;
      if (jmxManager == null) { // jmxManager property was not explicitly set so it defaults
        if (clientName.contains("locator")) {  // locators default to true
          thisMemberIsEligible = true;
        }
      } else {
        thisMemberIsEligible = jmxManager;
      }
      Log.getLogWriter().info("This member isManager: " + isManager + ", this member is eligible to be a manager: " + thisMemberIsEligible);
      if (isManager) {
        if (!thisMemberIsEligible) {
          throw new TestException("This member is a manager but it is not eligible");
        }
        boolean expectLocatorManagers = CommandPrms.getExpectLocatorManagers();
        if (expectLocatorManagers) { // only locators can be managers
          if (!clientName.contains("locator")){
            throw new TestException("This member is a manager and not a locator; expect managers only in locators");
          }
        } else { // locators cannot be managers
          if (clientName.contains("locator")) {
            throw new TestException("This member is a manager and a locator; expect manager only in non-locator members");
          }
        }
      }
      CommandBB.getBB().getSharedMap().put(isManagerKeyPrefix + RemoteTestModule.getMyVmid(), isManager);
    }
  }

  /** Verify that the distributed system has the correct number of managers
   *
   */
  public static void HydraTask_verifyAutoNumManagers() {
    // determine the number of locators and the number of cli jvms in this test
    int numLocators = 0;
    int numCliJvms = 0;
    Vector clientNames = TestConfig.tab().vecAt(ClientPrms.names);
    for (Object name: clientNames) {
      String nameStr = (String)name;
      if (nameStr.contains("locator")) {
        numLocators++;
      }
      if (nameStr.contains("cli")) {
        numCliJvms++;
      }
    }
    Log.getLogWriter().info("num locators " + numLocators + ", numCliJvms " + numCliJvms);

    // Iterate through BB map and gather information for validation
    Map aMap = CommandBB.getBB().getSharedMap().getMap();
    int isManagerCount = 0;
    int numEligible = 0;
    Set currentManagerSet = new HashSet();
    boolean jmxManagerWasSet = false;
    for (Object key: aMap.keySet()) {
      if ((key instanceof String) && ((String)key).startsWith(isManagerKeyPrefix)) {
        boolean isManager = (Boolean) aMap.get(key);
        if (isManager) {
          isManagerCount++;
          currentManagerSet.add(key);
        }
      }
      if ((key instanceof String) && ((String)key).startsWith(jmxManagerSettingKeyPrefix)) {
        jmxManagerWasSet = true;
        boolean jmxManager = (Boolean) aMap.get(key);
        if (jmxManager) {
          numEligible++;
        }
      }
    }

    if (numEligible == 0) { // either all were set to false or none were set at all
      if (!jmxManagerWasSet) { // none were set at all; this is the default case which means only locators implicitly are set to true
        numEligible = numLocators;
      }
    }
    Log.getLogWriter().info("Current number of managers: " + isManagerCount + ", " + currentManagerSet);
    Log.getLogWriter().info("num members eligible to be a manager: " + numEligible);

    // do the validation
    int expectedMaxNumManagers = Math.min(numCliJvms, numEligible);
    Log.getLogWriter().info("expectedMaxNumManagers is " + expectedMaxNumManagers);
    if (isManagerCount > expectedMaxNumManagers) {
      throw new TestException("Expected no more than " + expectedMaxNumManagers + " jmxManagers, but detected " + isManagerCount
                              + ": " + currentManagerSet);
    }
  }

  /** Verify nobody is a manager
   * 
   */
  public static void HydraTask_verifyNoManagers() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      Log.getLogWriter().info("This member is not a manager because it does not have a cache");
      return;
    } else {
      ManagementService service = ManagementService.getManagementService(theCache);
      boolean isManager = service.isManager();
      Log.getLogWriter().info("This member isManager: " + isManager);
      if (isManager) {
        throw new TestException("Expect " + service + ".isManager() to be false, but it is " + isManager);
      }
    }
  }
  
  /** Verify manager status for this member
   *
   */
  public static void HydraTask_verifyManagers() {
    String key = expectManagerKeyPrefix + RemoteTestModule.getMyVmid();
    Boolean expectManager = (Boolean) CommandBB.getBB().getSharedMap().get(key);
    if (expectManager == null) { // null is the same as false
      expectManager = false;
    }
    Log.getLogWriter().info("In HydraTask_verifyManager, expect this member to be a manager: " + expectManager);
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      Log.getLogWriter().info("This member is not a manager because it does not have a cache");
      if (expectManager) {
        throw new TestException("Expected this member to be a jmxManager, but it does not have a cache so can't be a jmxManager");
      }
    } else {
      ManagementService service = ManagementService.getManagementService(theCache);
      boolean isManager = service.isManager();
      Log.getLogWriter().info("This member isManager: " + isManager);
      if (expectManager.booleanValue() != isManager) {
        throw new TestException("Expect " + service + ".isManager() to be " + expectManager + ", but it is " + isManager);
      }
    }
  }

  /** Temporary until the product members can automatically become managers
   *
   */
  public static synchronized void HydraTask_becomeManager() {
    if (connectorService == null) { // not already a manager; see if we should become one
      if (CacheHelper.getCache() == null) {
        Log.getLogWriter().info("HydraTask_becomeManager-Returning without creating a manager because there is no cache in this member");
        return;
      }
      int vmId = RemoteTestModule.getMyVmid();
      Map managerMap = FederationBlackboard.getBB().getManagerONs();
      if ((managerMap == null) || (!managerMap.containsKey("vmId"+vmId))) { // this member was not a manager before; see if it should become one

        // Read the counter from the blackboard the number of managers that have been created
        long mgrCnt = CommandBB.getBB().getSharedCounters().incrementAndRead(CommandBB.managerCount);
        Log.getLogWriter().info("HydraTask_becomeManager-The current manager count is " + mgrCnt);

        // Read the param to determine how many managers to create
        String nbrOfManagers = CommandPrms.getNbrOfManagers();
        Log.getLogWriter().info("HydraTask_becomeManager-nbrOfManagers:" + nbrOfManagers);
        if (nbrOfManagers.indexOf("all") < 0) {
          try {
            // Try to convert the string to a number
            int nbrOfMgrsToCreate = Integer.parseInt(nbrOfManagers);
            Log.getLogWriter().info("HydraTask_becomeManager-nbrOfMgrsToCreate:" + nbrOfMgrsToCreate);
            // if we have a number, then we need to make sure we don't create too many managers
            if (mgrCnt > nbrOfMgrsToCreate) {
              Log.getLogWriter().info("HydraTask_becomeManager-Returning without creating a manager");
              return; // no need to create anymore managers
            }
          } catch (NumberFormatException ex) {
            throw new TestException("Invalid 'nbrOfManagers' option.  Must be a valid number or the word 'all'");
          }
        } else {
          // this member was already a manager previously but isn't now (connectorService is null)
          // this must be a restart of the member, so let it become a manager again
        }
      }

      Log.getLogWriter().info("HydraTask_becomeManager-Creating a manager");
      ManagementUtil.saveMemberManagerInBlackboard();
      ManagementService service = ManagementService.getManagementService(CacheHelper.getCache());
      service.startManager();
      HydraUtil.sleepForReplicationJMX();
      if(!ManagementUtil.checkIfCommandsAreLoadedOrNot()) {
        throw new TestException("Member failed to load any commands");
      }
      startRmiConnector();
    }
  }

  /** Temporary until the product does this automatically, start an RMI
   *  connector to later be used for gfsh to connect to.
   */
  protected static synchronized void startRmiConnector() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String vmId = "vmId" + RemoteTestModule.getMyVmid();
    try {
      LocateRegistry.createRegistry(port);
      JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + InetAddress.getLocalHost().getHostName()
                                            + ":" + port + "/" + "jmxrmi");
      connectorService = JMXConnectorServerFactory.newJMXConnectorServer(url, null, ManagementFactory.getPlatformMBeanServer());
      connectorService.start();
      Log.getLogWriter().info("JMX RMI Connector started at url " + url.toString());
      FederationBlackboard.getBB().addManagingNode(vmId, url.toString());
    } catch (RemoteException e) {
      throw new TestException("Could not start RMI Connector on " + vmId, e);
    } catch (MalformedURLException e) {
      throw new TestException("Could not start RMI Connector on " + vmId, e);
    } catch (UnknownHostException e) {
      throw new TestException("Could not start RMI Connector on " + vmId, e);
    } catch (IOException e) {
      throw new TestException("Could not start RMI Connector on " + vmId, e);
    }
  }

  /**
   * Method used for interactive debugging so that a test can start up members then
   *  sleep for a long time so we can run an interactive gfsh session against the
   *  members started by hydra.
   */
  public static void HydraTask_sleep() {
    int sleepForMs = CommandPrms.getSleepForMs();
    Log.getLogWriter().info("sleepForMs=" + sleepForMs);
    MasterController.sleepForMs(sleepForMs);
  }

  //=================================================
  // task methods

  /** Hydra task to initialize a TestableShell instance and connect.
   *  This replaces HydraTask_initShell().
   *
   */
  public synchronized static void HydraTask_initAndConnectCLI() {
    if (testInstance == null) {
      testInstance = new CommandTest();
    }
    if (testInstance.shell == null) {
      JMXManagerBlackboard.getInstance().print();
      testInstance.initShell();
      connectCLI();
    }
  }
  /** Hydra task to initialize a TestableShell instance and connect.
   *  This replaces HydraTask_initShell().
   *
   */
  public synchronized static void HydraTask_initCLI() {
    if (testInstance == null) {
      testInstance = new CommandTest();
    }
    if (testInstance.shell == null) {
      JMXManagerBlackboard.getInstance().print();
      testInstance.initShell();
    }
  }
  /** Hydra task to initialize a TestableShell instance and connect.
   *  This replaces HydraTask_initShell().
   *
   */
  public synchronized static void HydraTask_connectCLI() {
    if (testInstance != null) {
      connectCLI();
    }
  }

  /** Hydra task to initialize one testableShell per jvm and connect it to a
   *  random managing member.
   *
   *  NOTE: This is the old way to start TestableShell, which relied on 
   *        HydraTask_becomeManager. This is no longer needed now that 
   *        Gfsh can connect to locators and the product will automatically
   *        have members become managers.
   *
   */
  public synchronized static void HydraTask_initShell() {
    if (testInstance == null) {
      testInstance = new CommandTest();
    }
    if (testInstance.shell == null) {
      FederationBlackboard.getBB().printSharedMap();
      testInstance.initShell();
      testInstance.connectToManagingNode(testInstance.getRandomManagingMember());
    }
  }

  /** Hydra task for a serial deploy test
   *
   */
  public static void HydraTask_serialDeployTest() {
    testInstance.serialDeployTest();
  }

  /** Hydra task for a serial deploy test
   *
   */
  public static void HydraTask_deployTest() {
    testInstance.deployTest();
  }

  /** Hydra task for a serial deploy test
   *
   */
  public static void HydraTask_concDeployTest() {
    testInstance.concDeployTest();
  }

  /**
   * Hydra task to test the execution of all the various disconnected commands
   */
  public static void HydraTask_execDisconnectedCommands() {
    testInstance.basicCommandTest();
    testInstance.cmdVersionTest();
  }

    /**
    * Hydra task to test the execution of all the various connected commands
    */
  public static void HydraTask_execConnectedCommands() {
    testInstance.basicCommandTest();
    testInstance.cmdVersionTest();

    testInstance.cmdDescribeConfigTest();
    testInstance.cmdDescribeConnectionTest();
    testInstance.cmdDescribeDiskStoreTest();
    testInstance.cmdDescribeMemberTest();
    testInstance.cmdDescribeRegionTest();

    testInstance.cmdListDiskStoreTest(CommandPrms.getRegionsAdded());
    testInstance.cmdListMemberTest();
    testInstance.cmdListRegionTest();
    // Check to see if the functions have been registered yet
    testInstance.cmdListFunctionTest(CommandPrms.getFunctionsRegistered());
    // Check to see if the index has been added yet
    testInstance.cmdListIndexTest(CommandPrms.getIndexAdded());

    testInstance.cmdStatusLocatorTest();
    testInstance.cmdStatusServerTest();

    testInstance.cmdShowDeadLockTest();
    testInstance.cmdShowMetricsTest();

    testInstance.cmdExportStackTraceTest();
    //testInstance.cmdExportConfigTest();

    testInstance.cmdAlterDiskStoreTest();
    testInstance.cmdAlterRuntimeTest();

    testInstance.cmdCreateIndexTest();
    testInstance.cmdDestroyIndexTest();

    testInstance.destroyRegionTest();

    testInstance.encryptTest();

    testInstance.cmdPutTest();
    testInstance.cmdGetTest();
    testInstance.cmdLocateTest();
    testInstance.cmdRemoveTest();
  }

  public static void HydraTask_checkForCommandErrors() {
    checkForErrors(cmdErrorKey);
    checkForErrors(cmdErrorKey + "_thr" + RemoteTestModule.getCurrentThread().getThreadId());
  }
  
  public static void HydraTask_addFunction() {
    testInstance.addFunction();
  }
  /**
   * Hydra task to add an index to test the execution of the list index command
   */
  public static void HydraTask_addIndex() {
    testInstance.addIndex();
  }

  public static void HydraTask_CmdListDiskStoreTest() {
    testInstance.cmdListDiskStoreTest(CommandPrms.getRegionsAdded());
  }

  /**
   * Hydra task to exercise executing the help command
   */
  public static void HydraTask_verifyOfflineHelpCommand() {
    testInstance.verifyHelpCommand();
    testInstance.verifyHelpAvailability(false);
    testInstance.verifyHelpDescription();
  }
  /**
   * Hydra task to exercise executing the help command
   */
  public static void HydraTask_verifyOnlineHelpCommand() {
    testInstance.verifyHelpCommand();
    testInstance.verifyHelpAvailability(true);
    testInstance.verifyHelpDescription();
  }
  /**
   * Hydra task to exercise executing the help command
   */
  public static void HydraTask_verifyHelpForCommands() {
    testInstance.verifyHelpForCommands();
  }
  public static void HydraTask_checkForHelpErrors() {
    testInstance.checkForErrors(helpErrorKey);
  }

  /**
   * Hydra task to test the ....
   */
  public static void HydraTask_verifyDataTest() {
    testInstance.verifyDataTest();
    testInstance.verifyExportImportDataTest();
    testInstance.verifyShowMetricsDataTest();
  }
  public static void HydraTask_checkForDataErrors() {
    testInstance.checkForErrors(dataErrorKey);
  }

  public static void HydraTask_verifyCompressedDataTest() {
    testInstance.verifyCompressedDataTest();
  }
  public static void HydraTask_checkCompressedPerfStats() {
    testInstance.checkCompressedPerfStats();
  }

  /**
   * Hydra task to test the 'list member' & 'describe member' commands
   */
  public static void HydraTask_memberTests() {
    testInstance.cmdListMemberTest();
    testInstance.cmdDescribeMemberTest();
  }

  /**
   * Hydra task to test the 'list region' & 'describe region' commands
   */
  public static void HydraTask_regionTests() {
    testInstance.cmdListRegionTest();
    testInstance.cmdDescribeRegionTest();
  }

  /**
   * Hydra task to test the execution of the list function command
   */
  public static void HydraTask_listFunctionTest() {
    // Check to see if the functions have been registered yet
    boolean functionsAreRegistered = CommandPrms.getFunctionsRegistered();
    Log.getLogWriter().info("HydraTask_listFunctionTest-functionsAreRegistered=" + functionsAreRegistered);
    testInstance.cmdListFunctionTest(functionsAreRegistered);
  }

  /**
   * Hydra task to test the execution of the create index command
   */
  public static void HydraTask_createIndexTest() {
    testInstance.cmdCreateIndexTest();
  }

  /**
   * Hydra task to test the execution of the list index command
   */
  public static void HydraTask_listIndexTest() {
    // Check to see if the index has been added yet
    boolean indexesAreAdded = CommandPrms.getIndexAdded();
    Log.getLogWriter().info("HydraTask_listIndexTest-indexesAreAdded=" + indexesAreAdded);
    testInstance.cmdListIndexTest(indexesAreAdded);
  }

  public static void HydraTask_regionCommands() {
    testInstance.doRegionCommands();
  }

  /** Write the names of the regions defined in this member to the blackboard
   *
   */
  public static void HydraTask_writeRegionNamesToBB() {
    // Get all regions
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      return;
    }
    Set<Region<?, ?>> rootRegions = theCache.rootRegions();
    Set<Region> allRegions = new HashSet();
    allRegions.addAll(rootRegions);
    for (Region aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }

    // create a String set of all regions
    // create a Map of region names (key) and attributes (values)
    Set<String> regionNames = new HashSet();
    Map<String, RegionAttributes> regMap = new HashMap();
    for (Region aRegion: allRegions) {
      String regName = aRegion.getFullPath();
      regionNames.add(regName);
      AttributesFactory fac = new AttributesFactory(aRegion.getAttributes());
      RegionAttributes attr = fac.create();
      regMap.put(regName, fac.create());
    }

    // append the String set to the blackboard
    SharedLock lock = CommandBB.getBB().getSharedLock();
    lock.lock();
    try {
      Set<String> existingSet = (Set<String>) CommandBB.getBB().getSharedMap().get(regionNamesKey);
      if (existingSet == null) {
        existingSet = new HashSet<String>();
      }
      existingSet.addAll(regionNames);
      CommandBB.getBB().getSharedMap().put(regionNamesKey, existingSet);
      Map<String,RegionAttributes> existingMap = (Map<String,RegionAttributes>) CommandBB.getBB().getSharedMap().get(regionAttrKey);
      if (existingMap == null) {
        existingMap = new TreeMap<String, RegionAttributes>();
      }
      existingMap.putAll(regMap);
      CommandBB.getBB().getSharedMap().put(regionAttrKey, existingMap);
    } finally {
      lock.unlock();
    }
  }
  
  /** Write the MemberDescription for this member to the blackboard
  *
  */
 public static void HydraTask_writeMemberDescriptionToBB() {
   MemberDescription memDesc = new MemberDescription();
   memDesc.setVmID(RemoteTestModule.getMyVmid());
   memDesc.setMember(DistributedSystemHelper.getDistributedSystem().getDistributedMember());
   memDesc.setClientName(System.getProperty("clientName"));
   
   // Get all regions
   Cache theCache = CacheHelper.getCache();
   if (theCache == null) {
     return;
   }
   Set<Region<?, ?>> rootRegions = theCache.rootRegions();
   Set<Region> allRegions = new HashSet();
   allRegions.addAll(rootRegions);
   for (Region aRegion: rootRegions) {
     allRegions.addAll(aRegion.subregions(true));
   }

   // Update memDesc with all regions
   for (Region aRegion: allRegions) {
     memDesc.addRegion(aRegion);
   }
   
   // write to the blackboard
   SharedLock lock = CommandBB.getBB().getSharedLock();
   lock.lock();
   try {
     Map<Integer, MemberDescription> aMap = (Map<Integer, MemberDescription>) (CommandBB.getBB().getSharedMap().get(memberDescriptionKey));
     if (aMap == null) {
       aMap = new HashMap<Integer, MemberDescription>();
     }
     aMap.put(memDesc.getVmID(), memDesc);
     CommandBB.getBB().getSharedMap().put(memberDescriptionKey, aMap);
   } finally {
     lock.unlock();
   }
 }

  /** Disconnect from the DS
   */
  public static void HydraTask_disconnect() {
    DistributedSystemHelper.disconnect();
    testInstance = null;
    connectorService = null;
  }

  /** Clear fields in preparation to recreate managers with HydraTask_becomeManager. 
   *
   *  This is only used if it is desired that the managers be in potentially 
   *  different members than they were before during their first initialization, 
   *  otherwise this call can be skipped and if a member was a manager before it
   *  will become one again when HydraTask_becomeManager is called.
   */
  public static void HydraTask_clearManagers() {
    connectorService = null;
    FederationBlackboard.getBB().getSharedMap().remove("MANAGERS");
    CommandBB.getBB().getSharedCounters().zero(CommandBB.managerCount);
    CommandBB.getBB().getSharedCounters().zero(CommandBB.configurableInitCounter);
  }

  /** This is run from a "cli" member, which is not part of the ds but is there to
   *  execute cli commands from gfsh. It brings members in and out of the ds and
   *  to cause rebalance work to be done.
   */
  public static void HydraTask_rebalanceController() {
    testInstance.rebalanceController();
  }

  /** Loads regions with data. This is a batched task, and each thread will repeatedly
   *  run this task until it loads CommandPrms.numToLoadEachRegion into each region
   *  of the cache, then it will throw a StopSchedulingTaskOnclientOrder exception.
   */
  public static void HydraTask_load() {
    if (CacheHelper.getCache() == null) { // some tests are configured with no cache in a member
      throw new StopSchedulingTaskOnClientOrder("This member has no cache and cannot participate in loading regions with data");
    }
    int numToLoadEachRegion = CommandPrms.getNumToLoadEachRegion(); // number of keys to be put by each thread
    int CHUNK_SIZE = 50;
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> regionSet = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region<?, ?>> rootRegions = new HashSet(regionSet);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }

    boolean doneWithLoad = false;
    HashMap putAllMap = new HashMap();
    for (int i = 1; i <= CHUNK_SIZE; i++) {
      Object key = NameFactory.getNextPositiveObjectName();
      if (NameFactory.getCounterForName(key) <= numToLoadEachRegion) {
        Object value = new ValueHolder((String)key, rv);
        putAllMap.put(key, value);
      } else {
        doneWithLoad = true;
        break;
      }
    }
    if (putAllMap.size() > 0) {
      Log.getLogWriter().info("Created putAll map of size " + putAllMap.size());
      for (Region aRegion: regionSet) {
        Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
            aRegion.getFullPath());
        aRegion.putAll(putAllMap);
      }
    }
    if (doneWithLoad) {
      throw new StopSchedulingTaskOnClientOrder("Load is complete");
    }
  }
  
  public static void HydraTask_logRegions() {
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  public static void HydraTask_logBBs() {
    JMXManagerBlackboard.getInstance().print();
    CommandBB.getBB().print();
    PortBlackboard.getInstance().print();
    DistributedSystemBlackboard.getInstance().print();
  }

  public static void HydraTask_stopStart() {
    testInstance.stopStart();
  }
  
  /** Log whether this member is a manager or not
   *
   */
  public static void HydraTask_logManagerStatus() {
    CommandTestVersionHelper.logManagerStatus();
  }

  /** Verify the presence of MBeans
   *
   */
  public static void HydraTask_verifyMemberMBeans() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      Log.getLogWriter().info("Cache does not exist in this jvm so cannot verify MBeans");
    } else {
      ManagementService service = ManagementService.getExistingManagementService(theCache);
      if (service.isManager()) {
        // build a set of memberIds obtained from the blackboard; these memberIds are for members that have a cache
        Set<DistributedMember> membersFromBB = new HashSet();
        Map sharedMap = CommandBB.getBB().getSharedMap().getMap();
        for (Object key: sharedMap.keySet()) { // iterate through looking for memberIds
          if ((key instanceof String) && (((String)key).startsWith(memberWithCacheMemberIdKeyPrefix))) {
            membersFromBB.add((DistributedMember)(sharedMap.get(key)));
          }
        }
        Log.getLogWriter().info("Expect this member to have " + membersFromBB.size() + " member MBeans: " + membersFromBB);

        // obtain the MBeans from this manager member
        final long timeLimitMs = 120000; // how long to wait for MBeans to appear
        final int sleepTimeMs = 5000;
        long startTime = System.currentTimeMillis();
        int nullCount = 0;
        Map beanMap = new TreeMap();
        do {
          if (nullCount > 0) {
            Log.getLogWriter().info("Unable to obtain " + nullCount + " MBeans, sleeping for " + sleepTimeMs + " ms before trying again");
            MasterController.sleepForMs(sleepTimeMs);
          }
          nullCount = 0;
          beanMap = new TreeMap();
          for (DistributedMember member: membersFromBB) {
            ObjectName memberMBeanName = service.getMemberMBeanName(member);
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            MemberMXBean mbean = (MemberMXBean) JMX.newMBeanProxy(
              server, memberMBeanName, MemberMXBean.class, false);
            if (mbean == null) {
              nullCount++;
            }
            beanMap.put(member.getId(), mbean);
          }
          Log.getLogWriter().info("Obtained " + beanMap.size() + " MBeans: " + beanMap + " containing " + nullCount + " null MBeans");
        } while ((nullCount > 0) && (System.currentTimeMillis() - startTime < timeLimitMs));
        if (nullCount > 0) {
          Set missingBeanSet = new HashSet();
          for (Object key: beanMap.keySet()) {
            Object value = beanMap.get(key);
            if (value == null) {
              missingBeanSet.add(key);
            }
          }
          throw new TestException("Unable to obtain " + membersFromBB.size() + " member MBeans in " + timeLimitMs + " ms, missing " +
                                  missingBeanSet.size() + " member beans for members: " + missingBeanSet);
        }
      } else {
        Log.getLogWriter().info("This member is not a manager so cannot verify MBeans");
      }
    }
  }
  
  /** Verify the presence of Region MBeans and their values in this member.
   *
   */
  public static void HydraTask_verifyRegionMBeans() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      Log.getLogWriter().info("Cache does not exist in this jvm so cannot verify region MBeans");
    } else {
      verifyDistributedRegionMBeans();
      verifyLocalRegionMBeans();
    }
  }

  /** Return a Set of the the distributed region MBeans found in this member
   * 
   * @return A Set of the distributed region MBeans in this member.
   */
  private static Set<DistributedRegionMXBean> getDistributedRegionMBeans() {
    return getRegionMBeans("GemFire:service=Region,name=*,type=Distributed", DistributedRegionMXBean.class);
  }
  
  /** Return a Set of the the local region MBeans found in this member
   * 
   * @return A Set of the local region MBeans in this member.
   */
  private static Set<RegionMXBean> getLocalRegionMBeans() {
    return getRegionMBeans("GemFire:service=Region,name=*,type=Member,member=*", RegionMXBean.class);
  }
  
  /** Return a set of region MBeans in this member based on the given region bean query.
   * 
   * @param beanQueryStr The string to use to query region MBean names.
   * @return A set of region MBeans found in this member that satisfy the query.
   */
  private static Set getRegionMBeans(String beanQueryStr, Class beanClass) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectNamePattern;
    try {
      objectNamePattern = ObjectName.getInstance(beanQueryStr);
    } catch (MalformedObjectNameException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    Set<ObjectName> regionNames = server.queryNames(objectNamePattern, null);
    List regionMBeansList = new ArrayList();
    for (ObjectName name: regionNames) {
      Object mbean = JMX.newMXBeanProxy(server, name, beanClass, false);
      regionMBeansList.add(mbean);
    }
    Set regionMBeansSet = new HashSet(regionMBeansList);
    Log.getLogWriter().info("region ObjectNames, size " + regionNames.size() + ": " + setToString(regionNames));
    Log.getLogWriter().info("region MBeans, size " + regionMBeansSet.size() + ": " + setToString(regionMBeansSet));
    if (regionMBeansSet.size() != regionMBeansList.size()) {
      throw new TestException("Region MBeans (list) is size " + regionMBeansList.size() + ": " + regionMBeansList +
          ", but region MBeans (set) size is " + regionMBeansSet.size() + ": " + regionMBeansSet + "; expected equals sizes");
    }
    return regionMBeansSet;
  }
  
  /** Verify the set of distributed region MBeans in this member.
   * 
   */
  private static void verifyDistributedRegionMBeans() {
    ManagementService service = ManagementService.getExistingManagementService(CacheHelper.getCache());
    Log.getLogWriter().info("Verifying distributed region MBeans in this member, isManager: " + service.isManager());

    // verify the distributed region MBeans
    Set<DistributedRegionMXBean> regionMBeans = getDistributedRegionMBeans();
    if (service.isManager()) { // expect distributed region mbeans for all regions defined in the system
      StringBuffer errStr = new StringBuffer();
      Map<Integer, MemberDescription> memberDescMap = (Map<Integer, MemberDescription>) (CommandBB.getBB().getSharedMap().get(memberDescriptionKey));
      if (memberDescMap.size() == 0) {
        throw new TestException("Cannot do region MBean validation, CommandBB shared map does not have a value for key " + memberDescriptionKey 
            + "; test might need to add CommandTest.HydraTask_writeMemberDescriptionToBB() in all members that define regions");
      }
      Set<String> expectedRegionNames = new HashSet();
      for (MemberDescription memDesc: memberDescMap.values()) {
        expectedRegionNames.addAll(memDesc.getRegions());
      }
      verifyDistributedRegionMBeans(regionMBeans, expectedRegionNames);
    } else { // not a manager, should not find any region mbeans
      if (regionMBeans.size() > 0) {
        throw new TestException("Expected this member to not have any distributed region MBeans, but it has " + 
            regionMBeans.size() + ": " + regionMBeans);
      } else {
        Log.getLogWriter().info("Found " + regionMBeans.size() + " distributed region MBeans as expected");
      }
    }
    Log.getLogWriter().info("Done verifying presence of " + regionMBeans.size() + " distributed region MBeans");
    //xxx todo lynn add the following line in for distributed region mbeans (regions will not necessarily exist in this member)
    //verifyRegionMBeanValues(regionMBeans);
  }

  /** Verify the set of local region MBeans in this member.
   * 
   */
  private static void verifyLocalRegionMBeans() {
    ManagementService service = ManagementService.getExistingManagementService(CacheHelper.getCache());
    Log.getLogWriter().info("Verifying presence of local region MBeans in this member, isManager: " + service.isManager());
    if (service.isManager()) { 
      verifyLocalRegionMBeans_manager();
    } else { 
      verifyLocalRegionMBeans_nonManager();
    }
  }
  
  /** Verify local region MBeans in a manager member. 
   *  Expect to find a region MBean for each region defined in each member (so one MBean
   *  for each region/member pairing)
   */
  private static void verifyLocalRegionMBeans_manager() {
    Set<RegionMXBean> localRegionMBeans = getLocalRegionMBeans();
    
    // create a Map with key: member and value: List of MBeans for that member
    Map<String, Set<RegionMXBean>> memberMBeanMap = new HashMap();
    for (RegionMXBean MBean: localRegionMBeans) {
      String member = MBean.getMember();
      Set<RegionMXBean> MBeanSet = memberMBeanMap.get(member);
      if (MBeanSet == null) {
        MBeanSet = new HashSet();
        memberMBeanMap.put(member, MBeanSet);
      }
      MBeanSet.add(MBean);
    }
    StringBuilder aStr = new StringBuilder();
    for (String key: memberMBeanMap.keySet()) {
      Set value = (Set) memberMBeanMap.get(key);
      aStr.append(key + "(value is set containing " + value.size() + " elements)\n");
      aStr.append(setToString(value));
    }
    Log.getLogWriter().info("Verifying presence of local MBeans in this manager member: " + aStr.toString());
    
    // get the memberDescription map from the blackboard: key = vmID, value = MemberDescription
    Map<Integer, MemberDescription> memberDescMap = (Map<Integer, MemberDescription>) (CommandBB.getBB().getSharedMap().get(memberDescriptionKey));
    
    // verify that there are no extra or missing members found in the local region MBeans
    Log.getLogWriter().info("Looking for missing or extra members in " + localRegionMBeans.size() + " MBeans");
    Set<String> expectedMembers = new HashSet();
    for (MemberDescription memDesc: memberDescMap.values()) {
      if (memDesc.getRegions().size() > 0) { // blackboard indicates this member defined regions
        expectedMembers.add(memDesc.getMember().toString());
      }
    }
    Set<String> foundMembers = memberMBeanMap.keySet();
    Set<String> missingMembers = new HashSet(expectedMembers);
    missingMembers.removeAll(foundMembers);
    Set<String> extraMembers = new HashSet(foundMembers);
    extraMembers.removeAll(expectedMembers);
    if (missingMembers.size() > 0) {
      throw new TestException("local region MBeans in manager member is missing MBeans for members: " + missingMembers);
    }
    if (extraMembers.size() > 0) {
      throw new TestException("local region MBeans in manager member as unexpected MBeans for members: " + extraMembers);
    }
    
    // verify that we have an MBean for each region in the system for each member
    for (String member: memberMBeanMap.keySet()) { // iterate each member found for the local MBeans in this member
      Log.getLogWriter().info("Verifing local region MBeans for member " + member);
      Set<RegionMXBean> MBeanList = memberMBeanMap.get(member); // list of region MBeans for iteration variable "member"
      
      // get what is expected from the blackboard
      for (MemberDescription memDesc: memberDescMap.values()) {
        if (memDesc.getMember().equals(member)) { // found the MemberDescription for iteration variable "member"
          // get the regions defined in the member
          Set<String> expectedRegionFullPaths = memDesc.getRegions();
          verifyRegionMBeans(MBeanList, expectedRegionFullPaths);
        }
      }
    }
    Log.getLogWriter().info("Done verifying presence of " + localRegionMBeans.size() + " region MBeans");
    verifyRegionMBeanValues(localRegionMBeans);
  }

  /** Verify local region MBeans in a non-manager member.
   *  Expect to find MBeans only for regions defined in this member.
   * 
   */
  private static void verifyLocalRegionMBeans_nonManager() {
    Set<RegionMXBean> localRegionMBeans = getLocalRegionMBeans();
    
    // get expected region paths
    Set<Region> allRegions = getAllRegions();
    Set<String> expectedRegionPaths = new HashSet();
    for (Region aRegion: allRegions) {
      expectedRegionPaths.add(aRegion.getFullPath());
    }
    // verify there is an MBean for each expected region
    verifyRegionMBeans(localRegionMBeans, expectedRegionPaths);
    
    // verify each MBean has the correct member information; all MBeans should refer to this member
    String thisMember = DistributedSystemHelper.getDistributedSystem().getDistributedMember().toString();
    Log.getLogWriter().info("Verifying each of " + localRegionMBeans.size() + " has the correct member information");
    for (RegionMXBean MBean: localRegionMBeans) {
      String MBeanMember = MBean.getMember();
      if (!MBeanMember.equals(thisMember)) {
        throw new TestException("Expected region MBean " + MBean + " to reference member " + thisMember + " but it references " + MBeanMember);
      }
    }
    Log.getLogWriter().info("Done verifying presence of " + localRegionMBeans.size() + " region MBeans");
    verifyRegionMBeanValues(localRegionMBeans);
  }

  /** Return a Set of all regions defined in this member
   * 
   * @return A Set of all regions defined in this member.
   */
  private static Set<Region> getAllRegions() {
    Set<Region> regionSet = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region> rootRegions = new HashSet(regionSet);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    return regionSet;
  }
  
  /** Given a Set of region MBeans, and a Set of expected region name paths, verify that there is an MBean
   *  for each of the expected region paths.
   * @param regionMBeans A List of region MBeans.
   * @param expectedRegionPaths A Set of region full paths; there should be an MBean for each of these.
   */
  private static void verifyDistributedRegionMBeans(Set<DistributedRegionMXBean> regionMBeans, Set<String> expectedRegionPaths) {
    Set<String> MBeanRegionPaths = new HashSet();
    for (DistributedRegionMXBean regMBean: regionMBeans) {
      MBeanRegionPaths.add(regMBean.getFullPath());
    }
    _verifyRegionMBeans(MBeanRegionPaths, expectedRegionPaths);
  }

  /** Given a Set of region MBeans, and a Set of expected region name paths, verify that there is an MBean
   *  for each of the expected region paths.
   * @param regionMBeans A List of region MBeans.
   * @param expectedRegionPaths A Set of region full paths; there should be an MBean for each of these.
   */
  private static void verifyRegionMBeans(Set<RegionMXBean> regionMBeans, Set<String> expectedRegionPaths) {
    Set<String> MBeanRegionPaths = new HashSet();
    for (RegionMXBean regMBean: regionMBeans) {
      MBeanRegionPaths.add(regMBean.getFullPath());
    }
    _verifyRegionMBeans(MBeanRegionPaths, expectedRegionPaths);
  }
  
  private static void _verifyRegionMBeans(Set<String> MBeanRegionPaths, Set<String> expectedRegionPaths) {
    Log.getLogWriter().info("Expected " + expectedRegionPaths.size() + " regions with region MBeans: " + expectedRegionPaths);
    Log.getLogWriter().info("Found " + MBeanRegionPaths.size() + " region paths in MBeans: " + MBeanRegionPaths);

    // look for missing or extra region MBeans
    StringBuffer errStr = new StringBuffer();
    List<String> missing = new ArrayList(expectedRegionPaths);
    missing.removeAll(MBeanRegionPaths);
    List<String> extra = new ArrayList(MBeanRegionPaths);
    extra.removeAll(expectedRegionPaths);
    List<String> duplicates = new ArrayList(MBeanRegionPaths);
    for (String regPath: expectedRegionPaths) {
      duplicates.remove(regPath);
    }
    if (missing.size() != 0) {
      errStr.append("These regions do not have region MBeans in this member: " + missing + "\n");
    }
    if (extra.size() != 0) {
      errStr.append("Found extra region MBeans in this member for these regions: " + extra + "\n");
    }
    if (duplicates.size() != 0) {
      errStr.append("Region MBeans for these regions were found more than once in this member: " + duplicates + "\n");
    }
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
    Log.getLogWriter().info("Verified region path in " + MBeanRegionPaths.size() + " MBeans");
  }

  /** Given a set of region MBeans, verify the values in each MBean
   * 
   * @param regionMBeans The set of region MBeans to verify.
   */
  private static void verifyRegionMBeanValues(Set<RegionMXBean> regionMBeans) {
    for (RegionMXBean regionMBean: regionMBeans) {
      verifyRegionMBeanValues(regionMBean);
    }
  }

  /** Verify the values of the given region MBean
   * 
   * @param regionMBean The region MBean to verify.
   */
  private static void verifyRegionMBeanValues(RegionMXBean regionMBean) {
    Log.getLogWriter().info("Verifying values in region MBean " + regionMBean); 
    
    // as a check for the many methods on a RegioMXBean, use reflection to make sure all methods are covered
    // each method that is checked is removed from methodNames; at the end of this method we can see if there
    // are any methods left to cover
    Method[] methods = RegionMXBean.class.getDeclaredMethods();
    Set methodNames = new HashSet();
    for (Method aMethod: methods) {
      methodNames.add(aMethod.getName());
    }
    
    // get information to compare the bean values to; this is what is expected in the MBean
    ValidationInfo info = (ValidationInfo)(getValueFromFunction(regionMBean));
    Log.getLogWriter().info("For " + regionMBean + ", validation info is " + info);
    
    // verify each value in the region mBean; the expectedValue (while typed as Object) must
    // actually be the same class as the return value from the MBean for validation to work
    String methodName = "getEntryCount";
    Object expectedValue = new Long(info.localEntryCount);
    //verifyValue(expectedValue, methodName, regionMBean, "Bug 46866 (region " + info.regionFullPath + ") detected; ");
    verifyValue(expectedValue, methodName, regionMBean, "Bug 46991 detected; ");
    methodNames.remove(methodName);
    
    methodName = "getConfiguredRedundancy";
    expectedValue = (info.prAttr == null) ? -1 : (info.prAttr.getRedundantCopies());
    verifyValue(expectedValue, methodName, regionMBean, "Bug 46733 detected; ");
    methodNames.remove(methodName);
    
    methodName = "getRegionType";
    expectedValue = info.dataPolicy.toString();
    verifyValue(expectedValue, methodName, regionMBean, null);
    methodNames.remove(methodName);
    
    methodName = "getName";
    String regionFullPath = regionMBean.getFullPath();
    expectedValue = regionFullPath.substring(regionFullPath.lastIndexOf("/")+1);
    verifyValue(expectedValue, methodName, regionMBean, null);
    methodNames.remove(methodName);
    
    methodName = "isPersistentEnabled";
    expectedValue = info.withPersistence;
    verifyValue(expectedValue, methodName, regionMBean, null);
    methodNames.remove(methodName);
    
    methodName = "getParentRegion";
    expectedValue = info.parentRegionName;
    verifyValue(expectedValue, methodName, regionMBean, null);
    methodNames.remove(methodName);
    
    methodName = "getPrimaryBucketCount";
    expectedValue = info.primaryBucketCount;
    verifyValue(expectedValue, methodName, regionMBean, null);
    methodNames.remove(methodName);
    
    methodName = "isGatewayEnabled";
    expectedValue = info.gatewayEnabled;
    verifyValue(expectedValue, methodName, regionMBean, null);
    methodNames.remove(methodName);
    
    methodName = "getNumBucketsWithoutRedundancy";
    expectedValue = new Integer(0); // in this test all buckets have full redundancy  
    if (!info.dataPolicy.withPartitioning()) {
      expectedValue = new Integer(-1); // not partitioned, this is the default value
    }
    verifyValue(expectedValue, methodName, regionMBean, "Bug 46997 detected");
    methodNames.remove(methodName);
    
    String[] rates = new String[] { "getDestroyRate", "getDiskReadsRate", "getPutAllRate", "getLruDestroyRate",
        "getPutsRate", "getCreatesRate", "getDiskWritesRate", "getGetsRate" };
    // cannot include getLruEviction rate here because the act of validation can
    // cause the heap evictor to activate
    // making the lruEvictionRate non-zero; @todo lynn add better validation for
    // lruEvictionRate
    // rates need to be handled in a different test; too problematic here to know what to expect
//    expectedValue = new Float(0);
//    for (String rate : rates) {
//      verifyValue(expectedValue, rate, regionMBean, null);
//      methodNames.remove(rate);
//    }

    
    String[] prOnlyrates = new String[] { "getPutLocalRate", "getPutRemoteRate" };

    if(info.prAttr == null){
      expectedValue = new Float(-1);
    }else{
      expectedValue = new Float(0);
    }
    
    for (String prOnlyrate : prOnlyrates) {
      verifyValue(expectedValue, prOnlyrate, regionMBean, null);
      methodNames.remove(prOnlyrate);
    }

    verifyRegionAttributes(info, regionMBean);
    methodNames.remove("listRegionAttributes");
    
    verifyPartitionAttributes(info, regionMBean);
    methodNames.remove("listPartitionAttributes");
    
    verifyMembershipAttributes(info, regionMBean);
    methodNames.remove("listMembershipAttributes");
    
    verifyEvictionAttributes(info, regionMBean);
    methodNames.remove("listEvictionAttributes");
    
    Log.getLogWriter().info("xxx listFixedPartitionAttributes: " + regionMBean.listFixedPartitionAttributes());
    
    verifySubregions(info, regionMBean);
    methodNames.remove("listSubRegionPaths");

    // xxx lynn todo; make the following log to the error string 
    Log.getLogWriter().info("The following methods on RegionMXBean were not verified: " + methodNames);
  }
  
  /** Verify the listSubregion method on the given region MBean. Write any errors to the blackboard
   * 
   * @param info The information to validate against; this is what is expected.
   * @param regionMBean The region MBean to validate.
   */
  private static void verifySubregions(ValidationInfo info, RegionMXBean regionMBean) {
    // test list method with false argument
    String[] result = regionMBean.listSubregionPaths(false);

    Set<String> actual = new HashSet(Arrays.asList(result));
    Log.getLogWriter().info(regionMBean + ".listSubregionPaths(false): " + actual);
    Set<String> missing = new HashSet(info.directSubregions);
    missing.removeAll(actual);
    Set<String> extra = new HashSet(actual);
    extra.removeAll(info.directSubregions);
    if (missing.size() > 0) {
      String aStr = "The following subregions were missing from the result of " + regionMBean + ".listSubRegionPaths(false): " +
          missing + "; actual result is " + actual;
      Log.getLogWriter().info(aStr);
      saveErrorByThread(aStr);
    }
    if (extra.size() > 0) {
      String aStr = "The following extra subregions were found in the result of " + regionMBean + ".listSubRegionPaths(false), " +
          extra + "; actual result is " + actual;
      Log.getLogWriter().info(aStr);
      saveErrorByThread(aStr);
    }

    // test list method with true argument
    result = regionMBean.listSubregionPaths(true);

    actual = new HashSet(Arrays.asList(result));
    Log.getLogWriter().info(regionMBean + ".listSubregionPaths(false): " + actual);
    missing = new HashSet(info.allSubregions);
    missing.removeAll(actual);
    extra = new HashSet(actual);
    extra.removeAll(info.allSubregions);
    if (missing.size() > 0) {
      String aStr = "The following subregions were missing from the result of " + regionMBean + ".listSubRegionPath(true), " +
          missing + "; actual result is " + actual;
      Log.getLogWriter().info(aStr);
      saveErrorByThread(aStr);
    }
    if (extra.size() > 0) {
      String aStr = "The following extra subregions were found in the result of " + regionMBean + ".listSubRegionPaths(true), " +
          extra + "; actual result is " + actual;
      Log.getLogWriter().info(aStr);
      saveErrorByThread(aStr);
    }
  }

  /** Verify listRegionAttributes() method on the given region MBean
   * 
   * @param expected The information about the region that is expected.
   * @param regionMBean The region MBean to call listRegionAttributesOn()
   */
  private static void verifyRegionAttributes(ValidationInfo info,
      RegionMXBean regionMBean) {
    RegionAttributesData attr = regionMBean.listRegionAttributes();
    Log.getLogWriter().info("listRegionAttributes for " + regionMBean + " is: " + attr.toString());
    List<String> expectedInToString = new ArrayList();

    // as a check for the many methods on RegionAttributesData, use reflection to make sure all methods are covered
    // each method that is checked is removed from methodNames; at the end of this method we can see if there
    // are any methods left to cover
    Method[] methods = RegionAttributesData.class.getDeclaredMethods();
    Set methodNames = new HashSet();
    for (Method aMethod: methods) {
      methodNames.add(aMethod.getName());
    }

    // todo lynn xxx vary the attribute values in the test

    String methodName = "getCacheListeners";   
    Object expectedValue = info.cacheListeners;
    if (expectedValue != null) {
      String[] value = new String[info.cacheListeners.size()];
      for (int i = 0; i < info.cacheListeners.size(); i++) {
        value[i] = info.cacheListeners.get(i).getClass().getName();
      }
      expectedValue = value;
    }

    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("cacheListeners=" + Arrays.toString((String[])expectedValue));
    methodNames.remove(methodName);

    methodName = "getCacheLoaderClassName";
    expectedValue = info.cacheLoader;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("cacheLoaderClassName=" + expectedValue);
    methodNames.remove(methodName);

 
    methodName = "getCacheWriterClassName"; 
    expectedValue = info.cacheWriter;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("cacheWriterClassName=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getCompressor";
    expectedValue = info.compressor;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("compressor=" + expectedValue);
    methodNames.remove(methodName);
    
    methodName = "getConcurrencyLevel";
    expectedValue = info.concurrencyLevel;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("concurrencyLevel=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getCustomEntryIdleTimeout";
    expectedValue = info.customEntryIdleTimeout;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("customEntryIdleTimeout=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getCustomEntryTimeToLive";
    expectedValue = info.customEntryTimeToLive;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("customEntryTimeToLive=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getDataPolicy";
    expectedValue = info.dataPolicy;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("dataPolicy=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getDiskStoreName";
    expectedValue = info.diskStoreName;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("diskStoreName=" + expectedValue);
    methodNames.remove(methodName);



    methodName = "getEntryIdleTimeout";
    expectedValue = info.entryIdleTimeout;
    if (expectedValue != null) {
      expectedValue = info.entryIdleTimeout.getTimeout();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("entryIdleTimeout=" + expectedValue);
    methodNames.remove(methodName);
    
    methodName = "getEnableOffHeapMemory";
    expectedValue = info.enableOffHeapMemory;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("enableOffHeapMemory=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getEntryTimeToLive";
    expectedValue = info.entryTimeToLive;
    if (expectedValue != null) {
      expectedValue = info.entryTimeToLive.getTimeout();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("entryTimeToLive=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getGatewayHubId";
    expectedValue = info.gatewayHubId;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
      if (expectedValue.equals("")) {
        expectedValue = null; // RegionAttributesData displays attributes not set as null
      }
    }
    verifyValue(expectedValue, methodName, attr, "Bug 46987 detected; ");
    if (attr.getGatewayHubId() != null) {
      if ((attr.getGatewayHubId().equals("")) && (attr.toString().contains("gatewayHubId=null"))) {
        saveErrorByThread("Bug 46987 detected; getGatewayHubId returns empty String, but result of toString() on RegionAttributesData returns gatewayHubId=null" +
            ": result of toSring(): " + attr.toString());
      }
    }
    expectedInToString.add("gatewayHubId=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getInitialCapacity";
    expectedValue = info.initialCapacity;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("initialCapacity=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getInterestPolicy";
    expectedValue = info.interestPolicy;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("interestPolicy=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getKeyConstraintClassName";
    expectedValue = info.keyConstraint;
    if (expectedValue != null) {
      expectedValue = info.keyConstraint.getName();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("keyConstraintClassName=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getLoadFactor";
    expectedValue = info.loadFactor;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("loadFactor=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getPoolName";
    expectedValue = info.poolName;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("poolName=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getRegionIdleTimeout";
    expectedValue = info.regionIdleTimeout;
    if (expectedValue != null) {
      expectedValue = info.regionIdleTimeout.getTimeout();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("regionIdleTimeout=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getRegionTimeToLive";
    expectedValue = info.regionTimeToLive;
    if (expectedValue != null) {
      expectedValue = info.regionTimeToLive.getTimeout();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("regionTimeToLive=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getScope";
    expectedValue = info.scope;
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("scope=" + expectedValue);
    methodNames.remove(methodName);

 
    methodName = "getValueConstraintClassName";
    expectedValue = info.valueConstraint;
    if (expectedValue != null) {
      expectedValue = info.valueConstraint.getName();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("valueConstraintClassName=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isAsyncConflationEnabled";
    expectedValue = info.asyncConflationEnabled;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("asyncConflationEnabled=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isCloningEnabled";
    expectedValue = info.cloningEnabled;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("cloningEnabled=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isDiskSynchronous";
    expectedValue = info.diskSynchronous;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("diskSynchronous=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isGatewayEnabled";
    expectedValue = info.gatewayEnabled;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("gatewayEnabled=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isIgnoreJTA";
    expectedValue = info.ignoreJTA;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("ignoreJTA=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isIndexMaintenanceSynchronous";
    expectedValue = info.indexMaintenanceSynchronous;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("indexMaintenanceSynchronous=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isLockGrantor";
    expectedValue = info.lockGrantor;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("lockGrantor=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isMulticastEnabled";
    expectedValue = info.multicastEnabled;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("multicastEnabled=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "isStatisticsEnabled";
    expectedValue = info.statisticsEnabled;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("statisticsEnabled=" + expectedValue);
    methodNames.remove(methodName);

 
    methodName = "isSubscriptionConflationEnabled";
    expectedValue = info.subscriptionConflationEnabled;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("subscriptionConflationEnabled=" + expectedValue);
    methodNames.remove(methodName);
    
    methodName = "isHdfsWriteOnly";
    expectedValue = info.hdfsWriteOnly;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("hdfsWriteOnly=" + expectedValue);
    methodNames.remove(methodName);
    
    methodName = "getHdfsStoreName";
    expectedValue = info.hdfsStoreName;
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("hdfsStoreName=" + expectedValue);
    methodNames.remove(methodName);
    

    Collections.sort(expectedInToString);
    verifyAttrToString(attr.getClass().getSimpleName(), expectedInToString, attr.toString(), null);
    methodNames.remove("toString");

    verifyMethodNames(attr.getClass().getSimpleName(), methodNames);
  }

  /** Verify listPartitionAttributes() method on the given region MBean
   * 
   * @param expected The information about the region that is expected.
   * @param regionMBean The region MBean to call listPartitionAttributes() on
   */
  private static void verifyPartitionAttributes(ValidationInfo info,
                                                RegionMXBean regionMBean) {
    PartitionAttributesData attr = regionMBean.listPartitionAttributes();
    if (info.prAttr == null) { // regionMBean is not a PR
      if (attr != null) {
        saveErrorByThread("" + regionMBean + " does not represent a PR, but listPartitionAttributes() is " + attr);
      }
      return; // not a PR so no PR attributes to verify
    } else { // regionMBean is a PR
      if (attr == null) {
        saveErrorByThread("" + regionMBean + " represents a PR, but listPartitionAttributes() is null");
        return; // cannot go on, attributes is null
      }
    }
    Log.getLogWriter().info("listPartitionAttributes for " + regionMBean + " is: " + attr.toString());
    List<String> expectedInToString = new ArrayList();
   
    // as a check for the methods on PartitionAttributesData, use reflection to make sure all methods are covered
    // each method that is checked is removed from methodNames; at the end of this method we can see if there
    // are any methods left to cover
    Method[] methods = PartitionAttributesData.class.getDeclaredMethods();
    Set methodNames = new HashSet();
    for (Method aMethod: methods) {
      methodNames.add(aMethod.getName());
    }

    // todo lynn xxx vary the attribute values in the test
    String methodName = "getColocatedWith";
    Object expectedValue = info.prAttr.getColocatedWith();
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("colocatedWith=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getLocalMaxMemory";
    expectedValue = info.prAttr.getLocalMaxMemory();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("localMaxMemory=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getPartitionListeners";
    PartitionListener[] listArr = info.prAttr.getPartitionListeners();
    List aList = new ArrayList();
    for (PartitionListener listener: listArr) {
      aList.add(listener.getClass().getName());
    }
    expectedValue = aList.toArray(new String[0]);
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("partitionListeners=" + aList.toString());
    methodNames.remove(methodName);

    methodName = "getPartitionResolver";
    expectedValue = info.prAttr.getPartitionResolver();
    if (expectedValue != null) {
      expectedValue = info.prAttr.getPartitionResolver().getClass().getName();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("partitionResolver=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getRecoveryDelay";
    expectedValue = info.prAttr.getRecoveryDelay();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("recoveryDelay=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getRedundantCopies";
    expectedValue = info.prAttr.getRedundantCopies();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("redundantCopies=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getStartupRecoveryDelay";
    expectedValue = info.prAttr.getStartupRecoveryDelay();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("startupRecoveryDelay=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getTotalMaxMemory";
    expectedValue = info.prAttr.getTotalMaxMemory();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("totalMaxMemory=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getTotalNumBuckets";
    expectedValue = info.prAttr.getTotalNumBuckets();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("totalNumBuckets=" + expectedValue);
    methodNames.remove(methodName);

    // verify the toString() result on PartitionAttributesData
    verifyAttrToString(attr.getClass().getSimpleName(), expectedInToString, attr.toString(), null);
    methodNames.remove("toString");
    
    verifyMethodNames(attr.getClass().getSimpleName(), methodNames);

  }
  
  /** Verify listMembershipAttributes() method on the given region MBean
   * 
   * @param expected The information that is expected.
   * @param regionMBean The region MBean to call listMembershipAttributes() on
   */
  private static void verifyMembershipAttributes(ValidationInfo info,
                                                RegionMXBean regionMBean) {
    MembershipAttributesData attr = regionMBean.listMembershipAttributes();
    String allAttrStr = attr.toString();
    Log.getLogWriter().info("listMembershipAttributes for " + regionMBean + " is: " + allAttrStr);
    List<String> expectedInToString = new ArrayList();

    // as a check for the methods on MembershipAttributesData, use reflection to make sure all methods are covered
    // each method that is checked is removed from methodNames; at the end of this method we can see if there
    // are any methods left to cover
    Method[] methods = MembershipAttributesData.class.getDeclaredMethods();
    Set methodNames = new HashSet();
    for (Method aMethod: methods) {
      methodNames.add(aMethod.getName());
    }
    
    // todo lynn xxx vary the attribute values in the test

    String methodName = "getLossAction";
    Object expectedValue = info.membershipAttr.getLossAction();
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("lossAction=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getRequiredRoles";
    expectedValue = info.membershipAttr.getRequiredRoles();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("requiredRoles=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getResumptionAction";
    expectedValue = info.membershipAttr.getResumptionAction();
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("resumptionAction=" + expectedValue);
    methodNames.remove(methodName);
    
    // verify the toString() result on MembershipAttributesData
    verifyAttrToString(attr.getClass().getSimpleName(), expectedInToString, attr.toString(), null);
    methodNames.remove("toString");
    
    verifyMethodNames(attr.getClass().getSimpleName(), methodNames);
  }
  
  /** Verify listEvictionAttributes() method on the given region MBean
   * 
   * @param expected The information that is expected.
   * @param regionMBean The region MBean to call listEvictionAttributes() on
   */
  private static void verifyEvictionAttributes(ValidationInfo info,
                                               RegionMXBean regionMBean) {
    EvictionAttributesData attr = regionMBean.listEvictionAttributes();
    Log.getLogWriter().info("listEvictionAttributes for " + regionMBean + " is: " + attr.toString());
    List<String> expectedInToString = new ArrayList();

    // as a check for the methods on EvictionAttributesData, use reflection to make sure all methods are covered
    // each method that is checked is removed from methodNames; at the end of this method we can see if there
    // are any methods left to cover
    Method[] methods = EvictionAttributesData.class.getDeclaredMethods();
    Set methodNames = new HashSet();
    for (Method aMethod: methods) {
      methodNames.add(aMethod.getName());
    }
    Log.getLogWriter().info("EvictionAttributes is: " + info.evictionAttr);
    
    String methodName = "getAlgorithm";
    Object expectedValue = info.evictionAttr.getAlgorithm().toString();
    verifyValue(expectedValue, methodName, attr, null);
    expectedInToString.add("algorithm=" + expectedValue);
    methodNames.remove(methodName);

    methodName = "getAction";
    expectedValue = info.evictionAttr.getAction();
    if (expectedValue != null) {
      expectedValue = expectedValue.toString();
    }
    verifyValue(expectedValue, methodName, attr, null);
    if (!info.evictionAttr.getAlgorithm().isNone()) { // if the algorithm is set then we want to see action in the result of toString
      expectedInToString.add("action=" + expectedValue);
    }
    methodNames.remove(methodName);

    methodName = "getMaximum";
    EvictionAlgorithm algorithm = info.evictionAttr.getAlgorithm();
    expectedValue = null;
    if (algorithm.isLRUEntry() || algorithm.isLRUMemory()) {
      expectedValue = info.evictionAttr.getMaximum();
      expectedInToString.add("maximum=" + expectedValue);
    }
    verifyValue(expectedValue, methodName, attr, "Bug 46894 detected; ");
    methodNames.remove(methodName);
    
    // verify the toString() result on EvictionAttributesData
    verifyAttrToString(attr.getClass().getSimpleName(), expectedInToString, attr.toString(), "Bug 46895 detected; ");
    methodNames.remove("toString");
    
    verifyMethodNames(attr.getClass().getSimpleName(), methodNames);
  }
  
  /** Verify the list of method names not validated (should be empty)
   * 
   * @param className The name of the class containing the methods
   * @param methodNames A set of methodNames from classNames
   */
  protected static void verifyMethodNames(String className, Set methodNames) {
    if (methodNames.size() != 0) {
      String aStr = "The following methods on " + className + " were not verified: " + methodNames;
      Log.getLogWriter().info(aStr);
      String namesStr = methodNames.toString();
      if (namesStr.startsWith("set") || namesStr.contains(" set")) {
        saveErrorByThread("Bug 46834 (" + className + ") detected; API has public setter methods");
      } else {
        throw new TestException(aStr);
      }
    }
  }

  /** Given the result of a toString() call on an instance of a management attributes data class
   *  (*AttributesData), verify it.
   *  
   * @param attrClassName The name of the *AttributesData class
   * @param components The contents and value of the attributes expected in the string, in the order they should occur.
   * @param actual The result of toString()
   * @param errStrPrefix The prefix to an error, if actual is not as expected from the components argument.
   */
  private static void verifyAttrToString(String attrClassName, List<String> components, String actual, String errStrPrefix) {
    if (actual.contains(attrClassName + "@")) {
      saveErrorByThread("Bug 46787 (" + attrClassName + ") detected; toString() on " + attrClassName + " returned: " + actual);
      return;
    }
    if (actual.contains("{")) {
      saveErrorByThread("Bug 46831 (contains {) detected; " + actual);
      return;
    }
    if ((actual.contains("=,")) || (actual.contains("=]"))) {
      saveErrorByThread("Bug 46831 (contains empty string ',=' with no value) detected; " + actual);
      return;
    }

    // build the expected string
    StringBuilder expectedStr = new StringBuilder();
    expectedStr.append(attrClassName + " [");
    for (int i = 0; i < components.size(); i++) {
      expectedStr.append(components.get(i));
      if (i < components.size()-1) {
        expectedStr.append(", ");
      }
    }
    expectedStr.append("]");
    
    // verify
    if (actual.equals(expectedStr.toString())) {
      Log.getLogWriter().info("Verified: " + actual);
    } else {
      String result = formatExceptionTextDifference(attrClassName + ".toString()", actual, expectedStr.toString()).toString();
      if (errStrPrefix != null) {
        result = errStrPrefix + result;
      }
      Log.getLogWriter().info(result);
      saveErrorByThread(result);
    }
  }

  /** Execute the CliHelperFunction to retrieve a value from a remote member.
   * 
   * @param regionMBean This contains information about the remote member and region of interest.
   * @return The value requested by functionTask from the member and region specified in the regionMBean.
   */
  protected static Object getValueFromFunction(RegionMXBean regionMBean) {
    String member = regionMBean.getMember();
    Execution exe = FunctionService.onMember(getMemberFor(member));
    String[] args = new String[] {"" + RemoteTestModule.getCurrentThread().getThreadId(),
                                  regionMBean.getFullPath()};
    Log.getLogWriter().info("Executing " + exe + " with args " + Arrays.asList(args));
    ResultCollector<?, ?> result = exe.withArgs(args).execute(new CliHelperFunction());
    Object returnValue = ((ArrayList)(result.getResult())).get(0);
    return returnValue;
  }
  
  /** Given a distributed member string, look in the blackboard for the DistributedMember instance.
   * 
   * @param member The distributed member (as string) to find.
   * @return The DistributedMember instance for member.
   */
  private static DistributedMember getMemberFor(String member) {
    Log.getLogWriter().info("Looking for DistributedMember for " + member);
    Map<Integer, MemberDescription> aMap = (Map<Integer, MemberDescription>) (CommandBB.getBB().getSharedMap().get(memberDescriptionKey));
    for (MemberDescription memberDesc: aMap.values()) {
      if (memberDesc.getMember().toString().equals(member)) {
        Log.getLogWriter().info("Returning " + memberDesc.getMember());
        return memberDesc.getMember();
      }
    }
    throw new TestException("Could not find DistributedMember for " + member);
  }


  /** Verify the return value of the given method called on the base object. Any errors are written to the blackboard.
   * 
   * @param expectedValue The expected value of the result of the method.
   * @param methodName The method name to call on the baseObject.
   * @param baseObject The object on which to execute the method.
   * @param errStrPrefix Prefix the error text logged to errStr or null if none; this is used to flag a specific bug number,
   */
  private static void verifyValue(Object expectedValue, 
                                  String methodName, 
                                  Object baseObject, 
                                  String errStrPrefix) {
    try {
      Method aMethod = baseObject.getClass().getDeclaredMethod(methodName, (Class[])null);
      Object value = aMethod.invoke(baseObject, (Object[])null);
      boolean valueIsCorrect = false;
      if (expectedValue == null) {
        valueIsCorrect = (value == null);
      } else if (value == null) {
        valueIsCorrect = false; // expectedValue is not null
      } else {
        if (value.getClass().isArray() && expectedValue.getClass().isArray()) {
          valueIsCorrect = true;
          if (value.getClass().getComponentType() == expectedValue.getClass().getComponentType()) {
            Object[] valueArr = (Object[])value;
            Object[] expectedValArr = (Object[])expectedValue;
            if (valueArr.length == expectedValArr.length) {
              for (int i = 0; i < valueArr.length; i++) {
                if (!valueArr[i].equals(expectedValArr[i])) {
                 valueIsCorrect = false; 
                }
              }
            } else {
              valueIsCorrect = false;
            }
          } else {
            valueIsCorrect = false;
          }
        } else {
          valueIsCorrect = (expectedValue.equals(value));
        }
      }
      if (valueIsCorrect) {
        Log.getLogWriter().info(baseObject.getClass().getName() + "." + methodName + "() returned " +
            TestHelper.toString(value) + " as expected for " + baseObject);
      } else {
        String aStr = "";
        if (errStrPrefix != null) {
          aStr = errStrPrefix;
        }
        aStr = aStr + "Expected " + baseObject.getClass().getName() + "." + methodName + "() to return " + 
            TestHelper.toString(expectedValue) + 
            " but it returned " + TestHelper.toString(value) + 
            "; " + baseObject.getClass().getName() + " is " + baseObject;
        Log.getLogWriter().info(aStr);
        saveErrorByThread(aStr);
      }
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    }
  }
  
  /** Save an error string to the blackboard for this thread.
   * 
   * @param errStr The error string to save. 
   */
  private static void saveErrorByThread(String errStr) {
    Log.getLogWriter().info(errStr);
    final int ERROR_LIMIT = 30;
    final String LAST_ERROR_VALUE = "No more errors being logged to blackbooard, hit limit of " + ERROR_LIMIT;
    String bbKey = cmdErrorKey + "_thr" + RemoteTestModule.getCurrentThread().getThreadId();
    List<String> aList = (List) CommandBB.getBB().getSharedMap().get(bbKey);
    if (aList == null) {
      aList = new ArrayList();
      aList.add(errStr + "\n");
    } else if (aList.size() >= ERROR_LIMIT) {
      String lastValue = aList.get(aList.size()-1);
      if (lastValue.startsWith(LAST_ERROR_VALUE)) {
        return; // don't log any more
      } else {
        aList.add(LAST_ERROR_VALUE);
      }
    } else {
      if ((errStr.startsWith("Bug ")) && (errStr.contains("detected"))) {
        String bugDetectedStr = errStr.substring(0, errStr.indexOf("detected"));
        if (!aList.toString().contains(bugDetectedStr)) {
          aList.add(errStr + "\n");
        } // else we have already recorded this bug; skip this
      } else if (!aList.toString().contains(errStr)) {
        aList.add(errStr + "\n");
      }
    }
    CommandBB.getBB().getSharedMap().put(bbKey, aList);
  }

  /** Record in the blackboard if this member is a manager.
   *
   */
  public static void HydraTask_recordManagerStatus() {
    Cache theCache = CacheHelper.getCache();
    if (theCache != null) {
      ManagementService service = ManagementService.getExistingManagementService(theCache);
      if (service.isManager()) {
        SharedLock lock = CommandBB.getBB().getSharedLock();
        lock.lock();
        try {
          Map<Integer, String> vmIdMap = (Map<Integer, String>) CommandBB.getBB().getSharedMap().get(jmxManagerVmIdsKey);
          if (vmIdMap == null) {
            vmIdMap = new HashMap();
          }
          vmIdMap.put(RemoteTestModule.getMyVmid(), RemoteTestModule.getMyHost());
          CommandBB.getBB().getSharedMap().put(jmxManagerVmIdsKey, vmIdMap);
        } finally {
          lock.unlock();
        }
      }
    }
    Log.getLogWriter().info("Returning from HydraTask_recordManagerStatus, jmxManagers is " + CommandBB.getBB().getSharedMap().get(jmxManagerVmIdsKey));
  }

  /**
   * Hydra initialize task to initialize the Data Rate Verification Test
   * We need to:
   *    set the flag that controls running the data operations
   *    call the method that creates the Region(s)
   */
  public static void HydraTask_dataRateVerificationInit() {
    Log.getLogWriter().info("HydraTask_dataRateVerificationInit-Start");

    CommandBB.getBB().getSharedCounters().zero(CommandBB.dataRateDoOps);
    CommandBB.getBB().getSharedCounters().zero(CommandBB.dataRatePutCntr);
    testInstance.initDataRateRegion();

    Log.getLogWriter().info("HydraTask_dataRateVerificationInit-End");
  }

  /**
   * Hydra task to control the work flow for the Data Rate Verification Test
   */
  public static void HydraTask_dataRateController() {
    Log.getLogWriter().info("HydraTask_dataRateController-Start");

    // ensure that we have the correct (zero) values in our regions / members
    boolean haveErrors = testInstance.dataRateInitialVerification();
    if (!haveErrors) {
      // Turn on the flag to start doing region operatons
      CommandBB.getBB().getSharedCounters().increment(CommandBB.dataRateDoOps);
      // Check the data rate for a rate greater than zero.  Return back when found.
      testInstance.dataRateCheck(false);
      // Turn off the flag doing the region operatons
      CommandBB.getBB().getSharedCounters().zero(CommandBB.dataRateDoOps);

      // Check the data rate for a rate that is equal to zero.  Return back when found.
      testInstance.dataRateCheck(true);
    }

    long dataRatePutCntr = CommandBB.getBB().getSharedCounters().read(CommandBB.dataRatePutCntr);
    Log.getLogWriter().info("HydraTask_dataRateController-dataRatePutCntr=" + dataRatePutCntr);
    Log.getLogWriter().info("HydraTask_dataRateController-End");
  }

  public static void HydraTask_dataRateOperations() {
    Log.getLogWriter().info("HydraTask_dataRateOperations-Start");

    long dataRateDoOps = CommandBB.getBB().getSharedCounters().read(CommandBB.dataRateDoOps);
    if (dataRateDoOps > 0){
      testInstance.dataRateOperations();
    }

    Log.getLogWriter().info("HydraTask_dataRateOperations-End");
  }

  //=================================================
  // methods that do the work for tasks
  
  private void stopStart() {
    getManagerVmIds();
  }

  /** To be executed in a cli vm, using gfsh commands to determine the vmIds of managers
   * 
   */
  private Set<Integer>[] getManagerVmIds() {
//    // deploy a jar with a function that will help determine manager status
//    String jarPath = System.getProperty("JTESTS") + File.separator + ".." + File.separator + "extraJars" + File.separator + "CliHelper.jar";
//    String command = "deploy --group=all --jar=" + jarPath;
//    // execute the function to find out who is a manager
//    try {
//      execCommand(command);
//    } catch (TestException e) {
//      saveError(cmdErrorKey, new StringBuilder(TestHelper.getStackTrace(e)));
//    }
//
//    // execute the function to return the manager status and vmId for each member
//    Set<Integer> managerVmIds = new HashSet();
//    Set<Integer> nonManagerVmIds = new HashSet();
//    String result = execCommand("execute function --id=management.test.cli.CliHelperFunction" +
//        " --group=all" +
//        " --arguments=" + RemoteTestModule.getCurrentThread().getThreadId() + "," + CliHelperFunction.RETURN_MANAGER_STATUS);
//
//    // parse the result string to get the manager status from each member
//    String[] outputLines = result.split("\n");
//    for (String line: outputLines) {
//      String searchStr = "VmId=";
//      int index1 = line.indexOf(searchStr);
//      if (index1 >= 0) {
//        String subLine = line.substring(index1);
//        String[] tokens = subLine.split(" ");
//        // tokens[0] is "VmId=<int>", tokens[1] is "isManager=<boolean>"
//        int vmId = Integer.valueOf((tokens[0].split("="))[1]);
//        boolean isManager = Boolean.valueOf((tokens[1].split("="))[1]);
//        if (isManager) {
//          managerVmIds.add(vmId);
//        } else {
//          nonManagerVmIds.add(vmId);
//        }
//      }
//    }
//    Log.getLogWriter().info("Manager VmIds: " + managerVmIds);
//    Log.getLogWriter().info("NonManager VmIds: " + nonManagerVmIds);
//    return new Set[] {managerVmIds, nonManagerVmIds};
    return new Set[0];
  }

  /**
   * Create the region(s) that will be used in the Data Rate Verification Test
   */
  private void initDataRateRegion() {
    Log.getLogWriter().info("initDataRateRegion-Start");

    // CREATE our region
    runDataCommand("create region --type=REPLICATE --name=dataRateVerificationRegion");
    execSleepCommad("5");

    Log.getLogWriter().info("initDataRateRegion-End");
  }

  /**
   * Check the values returned by various show metrics commands and ensure that they have the proper
   *  starting values.
   * @return - true if any of the values weren't correct
   */
  private boolean dataRateInitialVerification() {
    Log.getLogWriter().info("dataRateInitialVerification-Start");
    boolean haveErrors = false;
    String regionName = "dataRateVerificationRegion";

    // Check the initial values
    String command = "show metrics --categories=cache";
    ArrayList<String> cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "totalRegionCount"), "1");
    Log.getLogWriter().info("dataRateInitialVerification-Show Metrics-Plain");

    // Check the initial values (by Region)
    command = "show metrics --categories=cluster,region --region=/" + regionName;
    cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "region entry count"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "missCount"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "hitCount"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "hitRatio"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "getsRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "putsRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "createsRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "destroyRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "putAllRate"), "0");
    Log.getLogWriter().info("dataRateInitialVerification-Show Metrics-Region");

    // Check the initial values (by Member)
    // Using DESCRIBE, save one of the member names for later
    String memberName = getResultValue(runDataCommand("describe region --name=" + regionName), "Hosting Members", ":");
    command = "show metrics --categories=region --member=" + memberName;
    cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "totalRegionCount"), "1");
    verifyResultValue(command, getResultValue(cmdResultLines, "totalRegionEntryCount"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "createsRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "destroyRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "totalMissCount"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "totalHitCount"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "getsRate"), "0");
    verifyResultValue(command, getResultValue(cmdResultLines, "putsRate"), "0");
    Log.getLogWriter().info("dataRateInitialVerification-Show Metrics-Member");

    ArrayList<String> errorList = (ArrayList<String>) CommandBB.getBB().getSharedMap().get(dataErrorKey);
    if (errorList != null && errorList.size() > 0) {
      haveErrors = true;
    }

    Log.getLogWriter().info("dataRateInitialVerification-End-haveErrors=" + haveErrors);
    return haveErrors;
  }

  /**
   * Check the values returned by various show metrics commands and ensure that they have the proper
   *  rates.  Will continue to check the rates until certian conditions are met.
   * @param checkingForZeroRate - true will check for a rate of zero
   */
  private void dataRateCheck(boolean checkingForZeroRate) {
    Log.getLogWriter().info("dataRateCheck-Start");

    String regionName = "dataRateVerificationRegion";
    String cmdShowMetricsRegion = "show metrics --categories=cluster,region --region=/" + regionName;
    // Using DESCRIBE, save one of the member names for later
    String memberName = getResultValue(runDataCommand("describe region --name=" + regionName), "Hosting Members", ":");
    String cmdShowMetricsMember = "show metrics --categories=region --member=" + memberName;

    float createsRateRegion, createsRateMember, putsRateRegion, putsRateMember, getsRateRegion, getsRateMember, destroysRateRegion, destroysRateMember;
    boolean done = false;
    do {
      // Check the initial values (by Region)
      ArrayList<String> cmdResultLines = runDataCommand(cmdShowMetricsRegion);
      createsRateRegion = Float.parseFloat(getResultValue(cmdResultLines, "createsRate"));
      putsRateRegion = Float.parseFloat(getResultValue(cmdResultLines, "putsRate"));
      getsRateRegion = Float.parseFloat(getResultValue(cmdResultLines, "getsRate"));
      destroysRateRegion = Float.parseFloat(getResultValue(cmdResultLines, "destroyRate"));
      Log.getLogWriter().info("dataRateCheck-Show Metrics-Region");

      // Check the initial values (by Member)
      cmdResultLines = runDataCommand(cmdShowMetricsMember);
      createsRateMember = Float.parseFloat(getResultValue(cmdResultLines, "createsRate"));
      putsRateMember = Float.parseFloat(getResultValue(cmdResultLines, "putsRate"));
      getsRateMember = Float.parseFloat(getResultValue(cmdResultLines, "getsRate"));
      destroysRateMember = Float.parseFloat(getResultValue(cmdResultLines, "destroyRate"));
      Log.getLogWriter().info("dataRateCheck-Show Metrics-Member");

      if (checkingForZeroRate) {
        //done = createsRateRegion == 0 && createsRateMember == 0 && putsRateRegion == 0 && putsRateMember == 0 && getsRateRegion == 0 && getsRateMember == 0 && destroysRateRegion == 0 && destroysRateMember == 0;
        done = createsRateRegion == 0 && createsRateMember == 0 && putsRateRegion == 0 && putsRateMember == 0 && getsRateRegion == 0 && getsRateMember == 0;
      } else {
        //done = createsRateRegion > 0 && createsRateMember > 0 && putsRateRegion > 0 && putsRateMember > 0 && getsRateRegion > 0 && getsRateMember > 0 && destroysRateRegion > 0 && destroysRateMember == 0;
        done = createsRateRegion > 0 && createsRateMember > 0 && putsRateRegion > 0 && putsRateMember > 0 && getsRateRegion > 0 && getsRateMember > 0;
      }
    } while (!done);

    Log.getLogWriter().info("dataRateCheck-End");
  }

  /**
   * Puts a specified number of entries (random values) to each region
   */
  private void dataRateOperations() {
    Log.getLogWriter().info("dataRateOperations-Start");

    String keyPrefix = "Object_";
    int nbrToPut = 10, nbrToHit = 5;
    RandomValues someRandomValues = new RandomValues();
    HashMap putAllMap;
    Object key;
    long keyAsALong, maxKey;
    int theBigestKey;
    boolean keyFound = false;
    // Get the regions to perform operations on
    Set<Region<?, ?>> regionSet = new HashSet(CacheHelper.getCache().rootRegions());
    // Keep doing operations while the control flag is set
    while (CommandBB.getBB().getSharedCounters().read(CommandBB.dataRateDoOps) > 0) {
      // Create the values to put to each Region
      putAllMap = new HashMap();
      // Get the initial values to drive the while-loop
      key = NameFactory.getNextPositiveObjectName();
      keyAsALong = NameFactory.getCounterForName(key);
      // Set up the flag to stop the while-loop
      maxKey = keyAsALong + nbrToPut;
      while (keyAsALong < maxKey) {
        // The put to the Map
        putAllMap.put(key, new ValueHolder((String) key, someRandomValues));
        CommandBB.getBB().getSharedCounters().increment(CommandBB.dataRatePutCntr);
        // Update the driving values
        key = NameFactory.getNextPositiveObjectName();
        keyAsALong = NameFactory.getCounterForName(key);
      }
      // Loop through the regions performing various operations
      for (Region aRegion : regionSet) {
        // The putAll to the Region
        aRegion.putAll(putAllMap);

        // Perform an update (always the last key)
        Log.getLogWriter().info("dataRateOperations-key=" + key);
        aRegion.put(key, new ValueHolder((String) key, someRandomValues));
        // Perform a destroy (always the last key)
        //aRegion.destroy(key);

        theBigestKey = new Long(NameFactory.getPositiveNameCounter()).intValue() - 1;
        Log.getLogWriter().info("dataRateOperations-theBigestKey=" + theBigestKey);
        // Perform some gets (hits)
        for (int i = 0;i < nbrToHit;i++) {
          key = TestConfig.tab().getRandGen().nextInt(1, theBigestKey);
          aRegion.get(keyPrefix + key);
        }

        // Perform a get (miss)
        key = TestConfig.tab().getRandGen().nextInt(theBigestKey, theBigestKey + 50);  // miss it by 50!
        aRegion.get(keyPrefix + key);

        /*
        // Perform a destroy
        Set regionKeys = aRegion.keySet();
        do {
          key = TestConfig.tab().getRandGen().nextInt(1, theBigestKey);
          if (regionKeys.contains(keyPrefix + key)) {
            keyFound = true;
          }
        } while (!keyFound);
        Log.getLogWriter().info("dataRateOperations-key=" + key);
        aRegion.destroy(keyPrefix + key);
        */
      }
    }

    long dataRatePutCntr = CommandBB.getBB().getSharedCounters().read(CommandBB.dataRatePutCntr);
    Log.getLogWriter().info("dataRateOperations-dataRatePutCntr=" + dataRatePutCntr);
    Log.getLogWriter().info("dataRateOperations-End");
  }

  private void encryptTest() {
    Log.getLogWriter().info("encryptTest-Start");

    // Call the command without any parameters
    String expectedOutput = "Parameter \"password\" is required. Use \"help <command name>\" for assistance.";
    checkCommand(CMD_ENCRYPT_PASSWORD, expectedOutput);

    // Call the command with an invalid parameter and no value
    String invalid = " --invalid=";
    expectedOutput = "Parameter \"password\" is required. Use \"help <command name>\" for assistance.\n" +
                     "Parameter invalid is not applicable for " + CMD_ENCRYPT_PASSWORD.trim();
    checkCommand(CMD_ENCRYPT_PASSWORD + invalid, expectedOutput);

    // Encrypt a password and check results using decrypt
    String pword = "mypassword";
    String outStr = execCommand(CMD_ENCRYPT_PASSWORD + " --password=" + pword);

    String tword = PasswordUtil.decrypt("encrypted(" + outStr.trim() + ")");
    checkCommand(CMD_ENCRYPT_PASSWORD + invalid, expectedOutput);
    if (!tword.equals(pword)) {
      throw new TestException("Results of \"" + CMD_ENCRYPT_PASSWORD + "\" could not be decrypted.\n" +
                              "\"" + CMD_ENCRYPT_PASSWORD + "\" returned: " + outStr);
    }

    // Encrypt a password using continuation and check results using decrypt
    execCommandLine(CMD_ENCRYPT_PASSWORD + " \\");
    outStr = execCommand(" --password=" + pword);

    tword = PasswordUtil.decrypt("encrypted(" + outStr.trim() + ")");
    if (!tword.equals(pword)) {
      throw new TestException("Results of \"" + CMD_ENCRYPT_PASSWORD + "\" continuation test could not be decrypted.\n" +
                              "\"" + CMD_ENCRYPT_PASSWORD + "\" returned: " + outStr);
    }

    Log.getLogWriter().info("encryptTest-End");
  }

  private void cmdPutTest() {
    Log.getLogWriter().info("cmdPutTest-Start");

    runDataCommand("create region --type=REPLICATE --name=myTestRegion");
    execSleepCommad("5");

    // Check the command with a variety of bad options and values
    CmdOptionInfo keyOption = new CmdOptionInfo(PARM_KEY, "", "myKey1", true, false);
    CmdOptionInfo valueOption = new CmdOptionInfo(PARM_VALUE, "", "myValue1", true, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION, "", "myTestRegion", true, false);
    //CmdOptionInfo keyClassOption = new CmdOptionInfo("key-class", "Key is either empty or Null", "", false, false);
    //CmdOptionInfo valueClassOption = new CmdOptionInfo("value-class", "Key is either empty or Null", "", false, false);
    CmdOptionInfo skipIfExistsOption = new CmdOptionInfo("skip-if-exists", "", "", false, true);
    CmdOptionInfo[] options = { keyOption, valueOption, regionOption, skipIfExistsOption };
    checkNamedParams(CMD_PUT, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_PUT, options);

    // Test if the region option works with the leading slash ('/') and without
    checkRegionOption(CMD_PUT, new CmdOptionInfo[] { keyOption, valueOption }, regionOption, false);
    checkRegionOption(CMD_PUT, new CmdOptionInfo[] { keyOption, valueOption }, regionOption, true);

    Log.getLogWriter().info("cmdPutTest-End");
  }

  private void cmdGetTest() {
    Log.getLogWriter().info("cmdGetTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo keyOption = new CmdOptionInfo(PARM_KEY, "", "myKey1", true, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION, "", "myTestRegion", true, false);
    //CmdOptionInfo keyClassOption = new CmdOptionInfo("key-class", "Key is either empty or Null", "", false, false);
    //CmdOptionInfo valueClassOption = new CmdOptionInfo("value-class", "Key is either empty or Null", "", false, false);
    CmdOptionInfo[] options = { keyOption, regionOption };
    checkNamedParams(CMD_GET, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_GET, options);

    // Test if the region option works with the leading slash ('/') and without
    checkRegionOption(CMD_GET, new CmdOptionInfo[] { keyOption }, regionOption, false);
    checkRegionOption(CMD_GET, new CmdOptionInfo[] { keyOption }, regionOption, true);

    Log.getLogWriter().info("cmdGetTest-End");
  }

  private void cmdLocateTest() {
    Log.getLogWriter().info("cmdLocateTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo keyOption = new CmdOptionInfo(PARM_KEY, "", "myKey1", true, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION, "", "myTestRegion", true, false);
    //CmdOptionInfo keyClassOption = new CmdOptionInfo("key-class", "Key is either empty or Null", "", false, false);
    //CmdOptionInfo valueClassOption = new CmdOptionInfo("value-class", "Key is either empty or Null", "", false, false);
    CmdOptionInfo recursiveOption = new CmdOptionInfo("recursive", "Value \"%1\" is not applicable for \"recursive\".", "", false, false);
    CmdOptionInfo[] options = { keyOption, regionOption, recursiveOption };
    checkNamedParams(CMD_LOCATE_ENTRY, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_LOCATE_ENTRY, options);

    // Test if the region option works with the leading slash ('/') and without
    checkRegionOption(CMD_LOCATE_ENTRY, new CmdOptionInfo[] { keyOption }, regionOption, false);
    checkRegionOption(CMD_LOCATE_ENTRY, new CmdOptionInfo[] { keyOption }, regionOption, true);

    Log.getLogWriter().info("cmdLocateTest-End");
  }

  private void cmdRemoveTest() {
    Log.getLogWriter().info("cmdRemoveTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION, "", "myTestRegion", true, false);
    CmdOptionInfo allOption = new CmdOptionInfo("all", "", "", false, true);
    CmdOptionInfo keyOption = new CmdOptionInfo(PARM_KEY, "Result    : false", "myKey1", false, false);
    //CmdOptionInfo keyClassOption = new CmdOptionInfo("key-class", "Key is either empty or Null", "", false, false);
    CmdOptionInfo[] options = { regionOption, allOption, keyOption };
    checkNamedParams(CMD_REMOVE, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_REMOVE, options);

    // Test if the region option works with the leading slash ('/') and without
    checkRegionOption(CMD_REMOVE, regionOption, false);
    checkRegionOption(CMD_REMOVE, regionOption, true);

    Log.getLogWriter().info("cmdRemoveTest-End");
  }

  /** Create work for rebalance by stopping members (and rebalancing)
   *  then restarting them (and rebalancing).
   */
  private void rebalanceController() {
    final int msToSleep = 20000;
    GsRandom gsRand = TestConfig.tab().getRandGen();

    // sleep to allow ops to run
    Log.getLogWriter().info("Sleeping for " + msToSleep + "ms to allow ops to run");
    MasterController.sleepForMs(msToSleep);

    // stop some vms so rebalance will have work to do
    // get the members that are candidates for stopping
    List<ClientVmInfo> candidateVMs = StopStartVMs.getAllVMs(); // this includes peers and accessors only, and not myself
    for (int i = 0; i < candidateVMs.size(); i++) { // remote the member this gfsh shell is connected to
      ClientVmInfo info = candidateVMs.get(i);
      if (info.getVmid() == shellManagerMemberVmId) {
        candidateVMs.remove(i);
      }
    }

    // from the candidate list, choose the members to stop and stop them
    int numToStop = CommandPrms.getNumToStop();
    List<ClientVmInfo> targetVMs = new ArrayList();
    targetVMs.addAll(candidateVMs);
    while (targetVMs.size() > numToStop) {
      targetVMs.remove(gsRand.nextInt(0, targetVMs.size()-1));
    }
    List<String> stopModeList = new ArrayList();
    for (int i = 1; i <= targetVMs.size(); i++) {
      stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    }
    StopStartVMs.stopVMs(targetVMs, stopModeList);

    // rebalance
    String rebResult = execCommand("rebalance");
    checkRebalanceResult(rebResult);

    // sleep after the stop
    Log.getLogWriter().info("Sleeping for " + msToSleep + "ms after stopping " + targetVMs);
    MasterController.sleepForMs(msToSleep);

    // restart the members to give rebalance work to do
    StopStartVMs.startVMs(targetVMs);

    // rebalance
    rebResult = execCommand("rebalance");
    checkRebalanceResult(rebResult);
  }

  /** Do validation on rebalance results
   * 
   * @param rebResult
   */
  private void checkRebalanceResult(String rebResult) {
    // @todo lynn add validation for units on time stats reported in result
    StringBuffer errStr = new StringBuffer();
    
    // get a list of all PRs defined in the test
    List<String> expectedRegNames = new ArrayList();
    Map<String,RegionAttributes> attrMap = (Map<String,RegionAttributes>) CommandBB.getBB().getSharedMap().get(regionAttrKey);
    for (String regName: attrMap.keySet()) {
      RegionAttributes attr = attrMap.get(regName);
      if (attr.getDataPolicy().withPartitioning()) {
        expectedRegNames.add(regName);
      }
    }
    
    // look for a section of rebalance result stats that do not name a region
    List<String> actualRegNames = new ArrayList();
    String[] lineArr = rebResult.split("\n");
    for (int i = 0; i < lineArr.length; i++) {
      String line = lineArr[i];
      if (line.contains("Rebalanced partition regions")) {
        final int numWordsBeforeNamedRegions = 3; // 3 words "Rebalance" "partition" "regions"...specific region names comes after this
        String[] wordArr = line.split("[\\s]+"); // split on white space
        for (int j = numWordsBeforeNamedRegions; j < wordArr.length; j++) {
          actualRegNames.add(wordArr[j]);
        }
        if (wordArr.length <= numWordsBeforeNamedRegions) { // we only found "Rebalanced partition regions" and no regions were named
          errStr.append("Bug 45567 detected; rebalance results contains rebalance stats without naming any regions in line " +
             (i+1) + " of output, line is |" + line + "| (see logs for full rebalance output\n");
        }
      }
    }
    
    // check that all the PRs were mentioned in the rebalance results
    Set actualRegNamesSet = new HashSet();
    actualRegNamesSet.addAll(actualRegNames);
    Set missingRegNames = new HashSet();
    missingRegNames.addAll(expectedRegNames);
    missingRegNames.removeAll(actualRegNamesSet);
    Set extraRegNames = new HashSet();
    extraRegNames.addAll(actualRegNames);
    extraRegNames.removeAll(expectedRegNames);
    if (missingRegNames.size() != 0) {
      errStr.append("The following partitioned regions were not present in rebalance results: " + missingRegNames + " \n");
    }
    if (extraRegNames.size() != 0) {
      errStr.append("The following partitioned regions were unexpected in rebalance results: " + extraRegNames + "\n");
    }
    
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
  }

  private void doRegionCommands() {

    // todo lynn
    //execCommand("describe region /");
    //    execCommand("describe region *");
    //    execCommand("describe region ///");
    //    execCommand("describe region");
    //    execCommand("describe region    ");
    //    execCommand("describe region        ");
    //    execCommand("describe region #$&*()");
    //    execCommand("describe region /nonExistentRegion");

    execCommand(CMD_LIST_MEMBERS);
    execCommand(CMD_LIST_REGIONS);
    execCommand(CMD_LIST_DISKSTORES);
    execCommand("list deployed");
    execCommand(CMD_LIST_FUNCTIONS);
    // todo lynn not implemented execCommand("list gateway");
    execCommand(CMD_LIST_INDEXES);
    execCommand("version");

    Set<String> allRegions = (Set<String>) CommandBB.getBB().getSharedMap().get(regionNamesKey);
    if (allRegions != null) {
      for (String regionName: allRegions) {
        String regionNameWithSlash = regionName;
        String regionNameWithoutSlash = regionName;
        if (regionName.startsWith("/")) {
          regionNameWithoutSlash = regionName.substring(1);
        } else {
          regionNameWithSlash = "/" + regionName;
        }
        String commandWithSlash = "describe region --name=" + regionNameWithSlash;
        String commandWithoutSlash = "describe region --name=" + regionNameWithoutSlash;
        String presentationStrWithSlash = execCommand(commandWithSlash);
        String presentationStrWithoutSlash = execCommand(commandWithoutSlash);
        // todo lynn
        //        if (!presentationStrWithSlash.equals(presentationStrWithoutSlash)) {
        //          logStrDifferences(presentationStrWithSlash, presentationStrWithoutSlash);
        //          throw new TestException("Command output for \"" + commandWithSlash + "\"\n" + presentationStrWithSlash +
        //              "\ndoes not equal output for \"" + commandWithoutSlash + "\"\n" + presentationStrWithoutSlash);
        //        }
      }
    }
  }

  /** Log the differences between str1 and str2 to help debug when they are expected to be equal.
   *
   * @param str1
   * @param str2
   */
  private void logStrDifferences(String str1, String str2) {
    StringBuffer aStr = new StringBuffer();
    int str1Length = str1.length();
    int str2Length = str2.length();
    if (str1Length == str2Length) {
      aStr.append("Str1 and str2 are both of length " + str1Length);
    } else {
      aStr.append("Str1 length is " + str1Length + " and str2 length is " + str2Length + "\n");
    }
    for (int i = 0; i < Math.min(str1Length, str2Length); i++) {
      char ch1 = str1.charAt(i);
      char ch2 = str2.charAt(i);
      if (ch1 != ch2) {
        aStr.append("Str1 and str2 differ at index " + i + "\n");
        aStr.append("str1: " + str1 + "\n");
        aStr.append("str2: " + str2 + "\n");
      }
    }
    Log.getLogWriter().info(aStr.toString());
  }

  /** Log the execution number of this serial task.
   */
  protected static long  logExecutionNumber() {
    long exeNum = CommandBB.getBB().getSharedCounters().incrementAndRead(CommandBB.executionNumber);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
    return exeNum;
  }

  /** Method to do the work of the serial deploy test
   *    Initialize a testableShell to run gfsh.
   *    Execute the deploy command.
   *    Execute a function on all members to verify the deploy was successful.
   *    Stop and start the members.
   *    Again execute the function to verify that the deployed file was persistent.
   *    Execute the undeploy command.
   *    Execute a function to verify that the members no longer have the jar deployed.
   */
  private void serialDeployTest() {
    logExecutionNumber();

    // deploy a jar
    int numTimesToDeploy = 3;
    Log.getLogWriter().info("Deploying the same jar " + numTimesToDeploy + " times");
    String jtests = System.getProperty("JTESTS");
    String jarPath = jtests + File.separator + ".." + File.separator + "extraJars" + File.separator + "jar1.jar";
    String expectedDeployedFileName = JarDeployer.JAR_PREFIX + "jar1.jar#1";
    File deployedFile = new File(expectedDeployedFileName);
    Log.getLogWriter().info("jar path is " + jarPath);
    for (int i = 1; i <= numTimesToDeploy; i++) {
      String command = "deploy --jar=" + jarPath;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a member group in the deploy command
        String groupName = getAvailableGroupName();
        if (groupName != null) { // groups were defined in this run
          command = command + " --group=" + groupName;
        }
      }
      execCommand(command);
      if (!deployedFile.exists()) {
        throw new TestException("After deploy " + deployedFile.getAbsolutePath() + " does not exist");
      }
    }

    String result = execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=noSuchFunction");
    if (result.contains("locator")) {
      throw new TestException("Bug 46113 detected: Result of execute function shows execution attempt on a locator: " + result);
    }
    execCommand(CMD_LIST_FUNCTIONS);
    result = execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");
    if (result.contains("locator")) {
      throw new TestException("Bug 45697 detected: Result of execute function shows execution attempt on a locator: " + result);
    }
    if (result.contains("Could not execute")) {
      throw new TestException("Bug 45697 detected: " + result);
    }

    // restart members to make sure the deploy persists
    List<ClientVmInfo> targetList = StopStartVMs.getAllVMs();
    List<String> stopModeList = new ArrayList<String>();
    for (int i = 1; i <= targetList.size(); i++) {
      stopModeList.add("mean_kill");
    }
    StopStartVMs.stopStartVMs(targetList, stopModeList);

    // verify that the restarted members can still access the class(es) and the jar was persisted
    execCommand(CMD_LIST_FUNCTIONS);
    execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");

    // undeploy, since the members share a directory, this undeploys for all of them
    // make sure the function can no longer be executed
    // first a jar that does not exist
    String command = "undeploy --jar=xyz.jar";
    execCommand(command);
    // now the real jar
    command = "undeploy --jar=jar1.jar";
    execCommand(command);
    if (HostHelper.isWindows()) { // in windows when undeploy removes the jar the jar is still there until the jvm exits, but its size will be 0
      // exists will return true, so make sure the file size is 0
      long length = deployedFile.length();
      if (length != 0) {
        throw new TestException("After undeploying, expect " + deployedFile.getAbsolutePath() + " to be length 0, but it is " + length);
      }
    } else { // not windows
      if (deployedFile.exists()) {
        MasterController.sleepForMs(10000);
        if (deployedFile.exists()) {
           throw new TestException("After undeploy " + deployedFile.getAbsolutePath() + " still exists");
        }
      }
    }
    execCommand(CMD_LIST_FUNCTIONS);
    execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");

    // restart and verify the members cannot access the classes
    StopStartVMs.stopStartVMs(targetList, stopModeList);
    execCommand(CMD_LIST_FUNCTIONS);
    execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");
  }

  /** Disconnect the current member either by closing the cache or disconnecting from the distributed
   *  system. Set appropriate variables to null to allow for later reinitialization.
   */
  private void disconnectMyself() {
    if (TestConfig.tab().getRandGen().nextBoolean()) {
      Log.getLogWriter().info("Closing the cache...");
      CacheHelper.getCache().close();
      Log.getLogWriter().info("Done closing the cache");
    } else { // disconnect from the ds
      Log.getLogWriter().info("Disconnecting...");
      DistributedSystemHelper.disconnect();
      Log.getLogWriter().info("Done disconnecting");
    }
    testInstance = null;
    shell.stop();
    shell = null;
    connectorService = null;
  }

  /** Method to do the work of the serial deploy test
   *    Initialize a testableShell to run gfsh.
   *    Execute the deploy command.
   *    Execute a function on all members to verify the deploy was successful.
   *    Stop and start the members.
   *    Again execute the function to verify that the deployed file was persistent.
   *    Execute the undeploy command.
   *    Execute a function to verify that the members no longer have the jar deployed.
   */
  private void deployTest() {
    logExecutionNumber();

    // deploy a jar
    int numTimesToDeploy = 3;
    Log.getLogWriter().info("Deploying the same jar " + numTimesToDeploy + " times");
    String jtests = System.getProperty("JTESTS");
    String jarPath = jtests + File.separator + ".." + File.separator + "extraJars" + File.separator + "jar1.jar";
    String expectedDeployedFileName = JarDeployer.JAR_PREFIX +"jar1.jar#1";
    File deployedFile = new File(expectedDeployedFileName);
    Log.getLogWriter().info("jar path is " + jarPath);
    for (int i = 1; i <= numTimesToDeploy; i++) {
      String command = "deploy --jar=" + jarPath;
      execCommand(command);
      if (!deployedFile.exists()) {
        MasterController.sleepForMs(10000);
        if (!deployedFile.exists()) {
          throw new TestException("After deploy " + deployedFile.getAbsolutePath() + " does not exist");
        }
      }
    }

    String result = execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=noSuchFunction");
    if (result.contains("locator")) {
      throw new TestException("Bug 46113 detected: Result of execute function shows execution attempt on a locator: " + result);
    }
    execCommand(CMD_LIST_FUNCTIONS);
    result = execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");
    if (result.contains("locator")) {
      throw new TestException("Bug 45697 detected: Result of execute function shows execution attempt on a locator: " + result);
    }
    if (result.contains("Could not execute")) {
      throw new TestException("Bug 45697 detected: " + result);
    }

    // restart members to make sure the deploy persists
    List<ClientVmInfo> targetList = StopStartVMs.getAllVMs();
    List<String> stopModeList = new ArrayList<String>();
    for (int i = 1; i <= targetList.size(); i++) {
      stopModeList.add("mean_kill");
    }
    StopStartVMs.stopStartVMs(targetList, stopModeList);

    execCommand(CMD_LIST_FUNCTIONS);
    execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");

    // undeploy, since the members share a directory, this undeploys for all of them
    // but they will still be able to access the classes until they restart
    // verify the members can still access the classes
    // first a jar that does not exist
    String command = "undeploy --jar=xyz.jar";
    execCommand(command);
    // now the real jar
    command = "undeploy --jar=jar1.jar";
    execCommand(command);
    if (HostHelper.isWindows()) { // in windows when undeploy removes the jar the jar is still there until the jvm exits, but its size will be 0
      // exists will return true, so make sure the file size is 0
      long length = deployedFile.length();
      if (length != 0) {
        throw new TestException("After undeploying, expect " + deployedFile.getAbsolutePath() + " to be length 0, but it is " + length);
      }
    } else { // not windows
      if (deployedFile.exists()) {
        MasterController.sleepForMs(10000);
        if (deployedFile.exists()) {
          throw new TestException("After undeploy " + deployedFile.getAbsolutePath() + " still exists");
        }
      }
    }
    execCommand(CMD_LIST_FUNCTIONS);
    execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");

    // restart and verify the members cannot access the classes
    StopStartVMs.stopStartVMs(targetList, stopModeList);
    execCommand(CMD_LIST_FUNCTIONS);
    execCommand("execute function --" + CliStrings.EXECUTE_FUNCTION__ID + "=cli.DeployFcn1 --arguments=1,2");
  }

  /** Use what is recorded in the blackboard to find the number of members hosting a cache
   *  that are not locators.
   * @return
   */
  private int getNumMembersWithCache() {
    int count = 0;
    Map aMap = CommandBB.getBB().getSharedMap().getMap();
    for (Object key: aMap.keySet()) {
      if ((key instanceof String) && (((String)key).startsWith(memberWithCacheMemberIdKeyPrefix))) {
        String memberStr = aMap.get(key).toString();
        if (!memberStr.contains("locator")) {
          count++;
        }
      }
    }
    return count;
  }

  /** Method to do the work of the concDeployTest.
   *     Deploy a jar
   *     List deployed jars
   *     Undeploy a jar
   */
  private void concDeployTest() {
    logExecutionNumber();

    int numTimesToExecute = 10;
    String jtests = System.getProperty("JTESTS");
    String command = null;
    GsRandom rand = TestConfig.tab().getRandGen();
    String[] availableJars = new String[] {"jar1.jar", "jar2.jar", "jar3.jar"};

    for (int i = 1; i <= numTimesToExecute; i++) {
      // deploy a jar
      int randInt = rand.nextInt(0, availableJars.length-1);
      String jarName = availableJars[randInt];
      String jarPath = jtests + File.separator + ".." + File.separator + "extraJars" + File.separator + jarName;
      command = "deploy --jar=" + jarPath;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a member group in the deploy command
        String groupName = getAvailableGroupName();
        if (groupName != null) { // groups were defined in this run
          command = command + " --group=" + groupName;
        }
      }
      Log.getLogWriter().info("Deploying " + jarPath);
      execCommand(command);
      Log.getLogWriter().info("Done with command " + command);

      // list jars
      command = "list deployed";
      Log.getLogWriter().info("Listing deployed jars");
      execCommand(command);
      Log.getLogWriter().info("Done with command " + command);

      // undeploy a jar
      randInt = rand.nextInt(0, availableJars.length-1);
      jarName = availableJars[randInt];
      command = "undeploy --jar=" + jarName;
      Log.getLogWriter().info("Undeploying " + jarName);
      execCommand(command);
      Log.getLogWriter().info("Done with command " + command);
    }
  }

  /** Execute a  function
   *
   * @param fcnName The name of the function to execute
   * @param invokeWithId If true then invoke with the function id, otherwise invoke by
   *                     instantiating the function
   */
  protected void executeFunction(String fcnName, boolean invokeWithId) {
    List<Comparable> functionArgs = new ArrayList<Comparable>();
    functionArgs.add(RemoteTestModule.getCurrentThread().getThreadId());
    functionArgs.add("abc");
    Execution exe = FunctionService.onMembers(CacheHelper.getCache().getDistributedSystem()).withArgs(functionArgs);
    Log.getLogWriter().info("Invoking " + fcnName + " on all members " + " with args " + functionArgs);
    ResultCollector<?, ?> fcnResult = null;
    if (invokeWithId) { // functions were registered, execute with id string
      fcnResult = exe.execute("cli.DeployFcn1");
    } else {
      Function fcn = (Function) instantiateObject(1, fcnName);
      fcnResult = exe.execute(fcn);
    }
    Object result = fcnResult.getResult();
    Log.getLogWriter().info("Result of function is " + result);
  }

  /** Instantiate and return an object of the given className from the given version (1 or 2)
   *  in <checkoutDir>/testsVersions
   * @param versionNumber Refers to <checkoutDir>/testsVersions/versionN where N is 1 or 2
   * @param className The name of the class in the specified version
   * @return A new instance of the className from the given version.
   */
  protected Object instantiateObject(int versionNumber, String className) {
    // create an instance of the function to execute
    ClassLoader previousCL = Thread.currentThread().getContextClassLoader();
    try {
      ClassLoader cl = PdxTest.createClassLoader(versionNumber);
      Log.getLogWriter().info("Setting class loader in this jvm to " + cl);
      Thread.currentThread().setContextClassLoader(cl);
      try {
        Class aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        Constructor constructor = aClass.getConstructor();
        Object newObj = constructor.newInstance();
        return newObj;
      } catch (ClassNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (SecurityException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (NoSuchMethodException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalArgumentException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (InstantiationException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalAccessException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (InvocationTargetException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } finally {
      Log.getLogWriter().info("Setting class loader to previous: " + previousCL);
      Thread.currentThread().setContextClassLoader(previousCL);
    }
  }

  /** Execute a function to verify a jar was deployed on other members.
   *
   * @param vmId
   * @param availableClasses
   * @param unavailableClasses
   */
  protected void verifyClassAvailability(int vmId, String[] availableClasses, String[] unavailableClasses) {
    List<Object> functionArgs = new ArrayList<Object>();
    functionArgs.add(RemoteTestModule.getCurrentThread().getThreadId());
    functionArgs.add(DeployTestFunction.VERIFY_CLASS_AVAILABILITY);
    functionArgs.add(Arrays.asList(availableClasses));
    functionArgs.add(Arrays.asList(unavailableClasses));
    Execution exe = null;
    if (vmId == -1) { // run function on all members
      exe = FunctionService.onMembers(CacheHelper.getCache().getDistributedSystem()).withArgs(functionArgs);
      Log.getLogWriter().info("Invoking " + DeployTestFunction.class.getName() + " on all members " +
                              " with args " + functionArgs);
    } else { // run function on one member
      DistributedMember member = (DistributedMember)(CommandBB.getBB().getSharedMap().get(memberKeyPrefix+vmId));
      exe = FunctionService.onMember(CacheHelper.getCache().getDistributedSystem(), member).withArgs(functionArgs);
      Log.getLogWriter().info("Invoking " + DeployTestFunction.class.getName() + " on " + member +
                              " with args " + functionArgs);
    }
    ResultCollector<?, ?> fcnResult = exe.execute(new DeployTestFunction());
    Object result = fcnResult.getResult();
    Log.getLogWriter().info("Result of function is " + result);
  }

  private String runExecCmds(String command) throws TestException {
    String rtnString = null;
    try {
      rtnString = execCommand(command);
      Log.getLogWriter().info("All was good when executing the command '" + command + "'!");
    } catch (TestException e) {
//      Log.getLogWriter().error("An Exception was thrown while executing the following command '" + command + "'.", e);
      Log.getLogWriter().error("An Exception was thrown while executing the following command '" + command + "'.", e);
      throw new TestException("All runExecCmds should run clean.");
//      shell.clearEvents();
    }
    return rtnString;
  }

  /**
   * Verify help for all commands.
   *
   * @throws TestException If verification of help commands fails.
   */
  private void verifyHelpForCommands() throws TestException {
    Log.getLogWriter().info("verifyHelpForCommands-Start");

    checkHelpCommand(CMD_ALTER_DISKSTORE, CommandHelpText.alterDiskStoreCmdHelpText);
    checkHelpCommand(CMD_ALTER_REGION, CommandHelpText.alterRegionCmdHelpText);
    checkHelpCommand(CMD_ALTER_RUNTIME, CommandHelpText.alterRuntimeCmdHelpText);
    checkHelpCommand(CMD_CREATE_ASYNC_EVENT_QUEUE, CommandHelpText.createAsyncEventQueueCmdHelpText);
    checkHelpCommand(CMD_CREATE_DISK_STORE, CommandHelpText.createDiskStoreCmdHelpText);
    checkHelpCommand(CMD_CREATE_INDEX, CommandHelpText.createIndexCmdHelpText);
    checkHelpCommand(CMD_CLOSE_DURABLE_CQ, CommandHelpText.closeDurableCqCmdHelpText);
    checkHelpCommand(CMD_CLOSE_DURABLE_CLIENT, CommandHelpText.closeDurableClientCmdHelpText);
    checkHelpCommand(CMD_DESCRIBE_CONFIG, CommandHelpText.describeConfigCmdHelpText);
    checkHelpCommand(CMD_DESCRIBE_CONNECTION, CommandHelpText.describeConnectionCmdHelpText);
    checkHelpCommand(CMD_DESCRIBE_DISKSTORE, CommandHelpText.describeDiskStoreCmdHelpText);
    checkHelpCommand(CMD_DESCRIBE_MEMBER, CommandHelpText.describeMemberCmdHelpText);
    checkHelpCommand(CMD_DESCRIBE_REGION, CommandHelpText.describeRegionCmdHelpText);
    checkHelpCommand(CMD_DESTROY_INDEX, CommandHelpText.destroyIndexCmdHelpText);
    checkHelpCommand(CMD_DESTROY_REGION, CommandHelpText.destroyRegionCmdHelpText);
    checkHelpCommand(CMD_ENCRYPT_PASSWORD, CommandHelpText.encryptPasswordCmdHelpText);
    checkHelpCommand(CMD_EXPORT_CONFIG, CommandHelpText.exportConfigCmdHelpText);
    checkHelpCommand(CMD_EXPORT_STACKTRACES, CommandHelpText.exportStacksTraceCmdHelpText);
    checkHelpCommand(CMD_LIST_ASYNC_EVENT_QUEUE, CommandHelpText.listAsyncEventQueueCmdHelpText);
    checkHelpCommand(CMD_LIST_DISKSTORES, CommandHelpText.listDiskStoresCmdHelpText);
    checkHelpCommand(CMD_LIST_DURABLE_CQS, CommandHelpText.listDurableCqsCmdHelpText);
    checkHelpCommand(CMD_LIST_FUNCTIONS, CommandHelpText.listFunctionsCmdHelpText);
    checkHelpCommand(CMD_LIST_INDEXES, CommandHelpText.listIndexesCmdHelpText);
    checkHelpCommand(CMD_LIST_MEMBERS, CommandHelpText.listMembersCmdHelpText);
    checkHelpCommand(CMD_LIST_REGIONS, CommandHelpText.listRegionsCmdHelpText);
    checkHelpCommand(CMD_SHOW_DEADLOCKS, CommandHelpText.showDeadLocksCmdHelpText);
    checkHelpCommand(CMD_SHOW_METRICS, CommandHelpText.showMetricsCmdHelpText);
    checkHelpCommand(CMD_STATUS_LOCATOR, CommandHelpText.statusLocatorCmdHelpText);
    checkHelpCommand(CMD_STATUS_SERVER, CommandHelpText.statusServerCmdHelpText);
    checkHelpCommand(CMD_VERSION, CommandHelpText.versionCmdHelpText);
    checkHelpCommand(CMD_SHOW_SUBSCRIPTION_QUEUE_SIZE, CommandHelpText.showSubscriptionQueueSizeCmdHelpText);
    checkHelpCommand(CMD_GET, CommandHelpText.getCmdHelpText);
    checkHelpCommand(CMD_LOCATE_ENTRY, CommandHelpText.locateEntryCmdHelpText);
    checkHelpCommand(CMD_PUT, CommandHelpText.putCmdHelpText);
    checkHelpCommand(CMD_REMOVE, CommandHelpText.removeCmdHelpText);

    Log.getLogWriter().info("verifyHelpForCommands-End");
  }

  private void checkHelpCommand(String command, String expectedOutput) {
    // Call help on the command
    if (command == null) {
      command = "help";
    } else {
      command = "help " + command;
    }
    String outputText = execHelpCommad(command);
    if (outputText != null) {
      outputText = outputText;
      expectedOutput = splitAndTrim(expectedOutput);
    }
    if (!outputText.equals(expectedOutput)) {
      saveError(helpErrorKey, formatExceptionText(command, outputText, expectedOutput));
    }
  }

  /** Verify help commands; ensure they are sorted and there are no missing or extra commands
    *  from what we know about.
    *
    * @throws TestException If verification of help commands fails.
    */
  private void verifyHelpCommand() throws TestException {
    Log.getLogWriter().info("verifyHelpCommand-Start");
    final List<String> expectedCommands = Arrays.asList(CommandHelpText.expectedCommandsArr);

    // run help and extract the returned commands from the presentation string
    List<String> commandList = (List<String>) getHelpInfo()[0];

    // verify that the commands are in sorted order
    List<String> sortedCommandList = new ArrayList();
    sortedCommandList.addAll(commandList);
    Collections.sort(sortedCommandList);
    if (!commandList.equals(sortedCommandList)) {
      Log.getLogWriter().info("verifyHelpCommand-Commands returned from help:\n" + commandList);
      Log.getLogWriter().info("verifyHelpCommand-Sorted commands:\n" + sortedCommandList);
      for (int i = 0; i < commandList.size(); i++) {
        String command = commandList.get(i);
        String sortedCommand = sortedCommandList.get(i);
        if (!command.equals(sortedCommand)) {
          throw new TestException("The help commands should be sorted; the command returned from help at index " +
                                  i + " is " + command + ", but sorted commands at index " + i + " is " + sortedCommand);
        }
      }
      throw new TestException("Expected commands returned from help to be sorted; see logs for details");
    }
    Log.getLogWriter().info("verifyHelpCommand-Commands are in sorted order");

    // verify that there are no commands missing and no extra commands
    boolean error = false;
    String errorString = null;
    List missingCommands = new ArrayList();
    missingCommands.addAll(expectedCommands);
    missingCommands.removeAll(commandList);
    if (missingCommands.size() > 0) {
      error = true;
      errorString = "The following commands were missing from help:\n" + missingCommands;
    }
    List extraCommands = new ArrayList();
    extraCommands.addAll(commandList);
    extraCommands.removeAll(expectedCommands);
    if (extraCommands.size() > 0) {
      error = true;
      if (errorString == null) {
        errorString = "The following unexpected commands were found in help:\n" + extraCommands;
      } else {
        errorString += "\nThe following unexpected commands were found in help:\n" + extraCommands;
      }
    }
    if (error) {
      throw new TestException(errorString);
    } else {
      Log.getLogWriter().info("verifyHelpCommand-No missing or extra commands detected");
    }
    Log.getLogWriter().info("verifyHelpCommand-End");
  }

  private void verifyHelpAvailability(boolean connected) {
    Log.getLogWriter().info("verifyHelpAvailability-Start");

    Map<String, Boolean> availabilityMap = (Map<String, Boolean>) getHelpInfo()[1];
    Log.getLogWriter().info("verifyHelpAvailability-Help returned " + availabilityMap);

    Map<String, Boolean> expectedAvailMap;
    if (connected) {
      expectedAvailMap = CommandHelpText.BuildExpectedConnectedHelpAvailabilities();
    } else {
      expectedAvailMap = CommandHelpText.BuildExpectedDisconnectedHelpAvailabilities();
    }
    
    // Check the validity of the data
    StringBuffer errStr = new StringBuffer();
    for (String commandName: CommandHelpText.expectedCommandsArr) {
      Object avail = availabilityMap.get(commandName);
      if (avail == null) {
        errStr.append(commandName + " was missing from help with connected: " + connected + "\n");
      }
      boolean actualAvail = (Boolean)avail;
      boolean expectedAvail = (Boolean)expectedAvailMap.get(commandName);
      if (actualAvail != expectedAvail) {
        errStr.append("Incorrect command availability found for command " + commandName + " , expected " + (expectedAvail ? "Available" : "Not Available") +
            ", but found " + (actualAvail ? "Available" : "Not Available") + " when connected: " + connected + "\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    } else {
      Log.getLogWriter().info("verifyHelpAvailability-Command Availability is correct.  Connected=" + connected);
    }
    Log.getLogWriter().info("verifyHelpAvailability-End");
  }
  
  private void verifyHelpDescription() {
    Log.getLogWriter().info("verifyHelpDescription-Start");
    Map<String, String> expectedDescriptions = CommandHelpText.BuildExpectedHelpDescriptions();

    // run help and extract the returned commands from the presentation string
    Map<String, String> descriptionMap = (Map<String, String>) getHelpInfo()[2];
    StringBuffer errStr = new StringBuffer();
    for (String commandName: CommandHelpText.expectedCommandsArr) {
      Log.getLogWriter().info("Verifying description for command: " + commandName);
      Object desc = descriptionMap.get(commandName);
      if (desc == null) {
        errStr.append(commandName + " was missing from help\n");
        continue;
      }
      Object actualDesc = desc;
      Object expectedDesc = expectedDescriptions.get(commandName);
      if (expectedDesc == null) {
        errStr.append("Do not have expected description for " + commandName + "\n");
        continue;
      }
      if (!actualDesc.equals(expectedDesc)) {
        errStr.append("Incorrect command description found for command " + commandName +
            " |\nWas expecting:|" + expectedDesc +
            "|\n but received:|" + actualDesc + "|\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    } else {
      Log.getLogWriter().info("verifyDescription-Descriptions are correct");
    }

    Log.getLogWriter().info("verifyHelpDescription-End");
  }

  /** Return information about the main help output (help for all commands).
   *
   * @return [0] A List of commands in the order they occurred in the help output
   *         [1] A Map of commands (key) and availability as listed in help (Boolean,
   *             true = available)
   *         [2] A Map of commands (key) and descriptions (String)
   */
  private Object[] getHelpInfo() {
    String helpText = testInstance.execCommand("help");
    String[] helpLines = helpText.split("\n"); // split on white space
    List<String> commandList = new ArrayList();
    Map<String, Boolean> availabilityMap = new HashMap();
    Map<String, String> descriptionMap = new HashMap();
    String currentCommand = null;
    int helpLinesIndex = 0;
    while (helpLinesIndex < helpLines.length){
      String line = helpLines[helpLinesIndex];
      String thisLine = line.trim();

      if (thisLine.contains("vailable)")) { // find the command line
        int index = thisLine.indexOf("(");
        if (index < 0) {
          throw new TestException("Unexpected format of help line: " + line);
        }
        thisLine = thisLine.substring(0, index);
        thisLine = thisLine.trim();
        // Check for duplicate commands
        if (commandList.contains(thisLine)) {
          throw new TestException(thisLine + " occurs more than once in help output");
        }
        commandList.add(thisLine);
        availabilityMap.put(thisLine, !line.contains("Not Available"));
        currentCommand = thisLine;
        helpLinesIndex++;
      } else {
        if (currentCommand != null) {
          int currIndex = helpLinesIndex;
          helpLinesIndex++;
          String description = "";
          while (currentCommand != null) {
            if (currIndex < helpLines.length){
              String currLine = helpLines[currIndex].trim();;
              if (!currLine.contains("vailable)") && (currLine.length() > 0)) { // this is a description line
                description = description + " " + currLine;
                currIndex++;
              } else { // this is the next command; finish the description of the previous command
                description = description.trim();
                descriptionMap.put(currentCommand, description);
                helpLinesIndex = currIndex;
                currentCommand = null;
              }
            } else {
              currentCommand = null; // end the loop
            }
          }
        } else {
          helpLinesIndex++;
        }
      }
    }
    //    Log.getLogWriter().info("ordered commandList: " + commandList);
    //    Log.getLogWriter().info("availabilityMap: " + availabilityMap);
    Log.getLogWriter().info("descriptionMap is " + descriptionMap);
    return new Object[] {commandList, availabilityMap, descriptionMap};
  }

  /**
   * Method to do the work to test the version command
   */
  private void cmdVersionTest() {
    Log.getLogWriter().info("cmdVersionTest-Start");
    String parm_full = "full";

    // Check the command with a variety of bad options and values
    CmdOptionInfo fullOption = new CmdOptionInfo(parm_full, "", "", false, true);
    CmdOptionInfo[] options = { fullOption };
    checkNamedParams(CMD_VERSION, options);

    // Check the command with options specified on separate lines
    fullOption.setValidValue("true");
    checkMultiLineExecution(CMD_VERSION, options);

    // Call the command without any parameters
    checkCommand(CMD_VERSION, VERSION);

    // Call the command without any parameters but adding a semi-colon
    checkCommand(CMD_VERSION + ";", VERSION);

    parm_full = " --" + parm_full;
    // Call the command with full parameter and 'false' value
    checkCommand(CMD_VERSION + parm_full + "=false", VERSION);

    // Call the command with full parameter and no value
    String[][] expectedText = {
      { "Java version:", "Native version:", "Source revision:", "Source repository:", "Running on:" },
      { null, null, null, null, null }
    };
    String fullVersionOutputText = checkOutputLines(CMD_VERSION + parm_full, expectedText);

    // Call the command with full parameter and 'true' value
    checkCommand(CMD_VERSION + parm_full + "=true", fullVersionOutputText);

    Log.getLogWriter().info("cmdVersionTest-End");
  }

  /**
   * Method to do the work to test the ....
   */
  private void verifyDataTest() {
    Log.getLogWriter().info("verifyDataTest-Start");

    // LIST the regions to validate that our region doesn't exist
    String listRegionsCmd = "list regions";
    verifyResultContains(listRegionsCmd, runDataCommand(listRegionsCmd), "No Regions Found");

    // CREATE our region and verify that it was created (must follow create region command with a sleep command #46391)
    String regionName = "dataTestRegion";
    String createRegionCmd = "create region --type=REPLICATE --name=" + regionName;
    verifyResultContains(createRegionCmd, runDataCommand(createRegionCmd), "Region \"/" + regionName + "\" created on");
    saveError(dataErrorKey,
      new StringBuilder("Bug #46391 Detected - Must issue a 'sleep' command to ensure that the region was created prior to any further operations."));
    execSleepCommad("5");

    // Perform a Describe Region to ensure that we have zero entries
    String describeRegionCmd = "describe region --name=" + regionName;
    verifyResultValue(describeRegionCmd, getResultValue(runDataCommand(describeRegionCmd), "size"), "0");
    // Perform a Show Metrics to ensure that we have zero entries
    String showMetricsCmd = "show metrics --region=" + regionName;
    verifyResultValue(showMetricsCmd, getResultValue(runDataCommand(showMetricsCmd), "region entry count"), "0");

    // Add an entry to the Region
    String putCommand = "put --key=key1 --value=value1 --region=/" + regionName;
    ArrayList<String> cmdResultLines = runDataCommand(putCommand);
    // Check the results
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Old Value", ":"), "<NULL>");

    //Must issue a 'sleep' command to ensure that the MBean updates are reported.
    execSleepCommad("5");

    // Recheck the results
    // Perform a Describe Region to ensure that we have one entry
    verifyResultValue(describeRegionCmd, getResultValue(runDataCommand(describeRegionCmd), "size"), "1");
    // Perform a Show Metrics to ensure that we have one entry
    verifyResultValue(showMetricsCmd, getResultValue(runDataCommand(showMetricsCmd), "region entry count"), "1");

    String removeCommand = "remove --key=key1 --region=/" + regionName;
    cmdResultLines = runDataCommand(removeCommand);
    verifyResultValue(removeCommand, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(removeCommand, getResultValue(cmdResultLines, "Key  ", ":"), "key1");

    // Next we should DESTROY our region
    String command = "destroy region --name=/" + regionName;
    verifyResultContains(command, runDataCommand(command), "\"/" + regionName + "\"  destroyed successfully.");

    Log.getLogWriter().info("verifyDataTest-End");
  }

  /**
   * Method to do the work to test the followng commands:
   *   list regions, create region, describe region, put, export data, import data and get
   */
  private void verifyExportImportDataTest() {
    Log.getLogWriter().info("verifyExportImportDataTest-Start");

    // This is the region name that we will be using for this test
    String regionName = "exportImportTestRegion";
    String listRegionsCmd = "list regions";
    String createRegionCmd = "create region --type=REPLICATE --name=" + regionName;
    String describeRegionCmd = "describe region --name=" + regionName;

    // 1st LIST the regions to validate that our region doesn't exist
    verifyResultNotContains(listRegionsCmd, runDataCommand(listRegionsCmd), regionName);

    // 2nd, CREATE our region and verify that it was created (must follow create region command with a sleep command #46391)
    verifyResultContains(createRegionCmd, runDataCommand(createRegionCmd), "Region \"/" + regionName + "\" created on");
    saveError(dataErrorKey,
      new StringBuilder("Bug #46391 Detected - Must issue a 'sleep' command to ensure that the region was created prior to any further operations."));
    execSleepCommad("5");

    // 3rd, LIST the regions again to validate that our region now exists
    verifyResultContains(listRegionsCmd, runDataCommand(listRegionsCmd), regionName);

    // 4th, DESCRIBE our region to validate that it has a size of zero
    ArrayList<String> cmdResultLines = runDataCommand(describeRegionCmd);
    verifyResultValue(describeRegionCmd, getResultValue(cmdResultLines, "size"), "0");
    // Sava one of the member names for later
    String memberName = getResultValue(cmdResultLines, "Hosting Members", ":");

    // 5th, perform some PUTs to our region
    String command = "put --key=key1 --value=value1 --region=/" + regionName;
    cmdResultLines = runDataCommand(command);
    // Check the results
    verifyResultValue(command, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(command, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(command, getResultValue(cmdResultLines, "Old Value", ":"), "<NULL>");

    // Use GET to check to see if we have some data
    command = "get --key=key1 --region=/" + regionName;
    cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(command, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(command, getResultValue(cmdResultLines, "Value  ", ":"), "value1");

    for (int i = 1;i <= 10;i++) {
      runDataCommand("put --key=key" + i + " --value=value1 --region=/" + regionName);
    }

    // DESCRIBE our region again to ensure that the correct number of entries were 'put'
    verifyResultValue(describeRegionCmd, getResultValue(runDataCommand(describeRegionCmd), "size"), "10");

    // Now, EXPORT the data from our region and the member that we saved earlier
    command = "export data --file=myData.gfd --region=/" + regionName + " --member=" + memberName;
    cmdResultLines = runDataCommand(command);
    verifyResultContains(command, cmdResultLines, "Data succesfully exported from region : /" + regionName);
    verifyResultContains(command, cmdResultLines, "myData.gfd on host :");

    // Lets use REMOVE to prepare the region for the import
    command = "remove --region=/" + regionName + " --all";
    cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(command, getResultValue(cmdResultLines, "Message", ":"), "Cleared all keys in the region");

    // DESCRIBE our region again to ensure that all the entries were 'removed'
    verifyResultValue(describeRegionCmd, getResultValue(runDataCommand(describeRegionCmd), "size"), "0");

    // Now, IMPORT the data file we created earlier and validate success to our region
    command = "import data --file=myData.gfd --region=/" + regionName + " --member=" + memberName;
    cmdResultLines = runDataCommand(command);
    verifyResultContains(command, cmdResultLines, "to region : /" + regionName);

    // Next, DESCRIBE our region again to ensure that the correct number of entries were imported
    verifyResultValue(describeRegionCmd, getResultValue(runDataCommand(describeRegionCmd), "size"), "10");

    // Finally, use GET to check some of the values
    // try the first key
    command = "get --key=key1 --region=/" + regionName;
    cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(command, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(command, getResultValue(cmdResultLines, "Value  ", ":"), "value1");
    // try the sixth key
    command = "get --key=key6 --region=/" + regionName;
    cmdResultLines = runDataCommand(command);
    verifyResultValue(command, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(command, getResultValue(cmdResultLines, "Key  ", ":"), "key6");
    verifyResultValue(command, getResultValue(cmdResultLines, "Value  ", ":"), "value1");

    // Next we should DESTROY our region
    command = "destroy region --name=/" + regionName;
    verifyResultContains(command, runDataCommand(command), "\"/" + regionName + "\"  destroyed successfully.");

    // Again, LIST the regions to validate that our region has been removed
    verifyResultNotContains(listRegionsCmd, runDataCommand(listRegionsCmd), regionName);

    Log.getLogWriter().info("verifyExportImportDataTest-End");
  }

  private void verifyShowMetricsDataTest() {
    Log.getLogWriter().info("verifyShowMetricsDataTest-Start");
    final String totalHitCount = "totalHitCount";
    String regionName = "showMetricsDataTestRegion";

    // Loop through all of the regions to find the entry count of each and the total of all
    /*
    int totalSize = 0;
    for (String regionName : (Set<String>) CommandBB.getBB().getSharedMap().get(regionNamesKey)) {
      ArrayList<String> cmdResultLines = runDataCommand(describeRegionCmd + regionName);
      int entrySize = Integer.parseInt(getResultValue(cmdResultLines, "size"));
      Log.getLogWriter().info("verifyShowMetricsDataTest-" + regionName + " has a size of:" + entrySize);
      totalSize += entrySize;
    }
    Log.getLogWriter().info("verifyShowMetricsDataTest-The size of All regions is:" + totalSize);
    */

    // Check the initial values
    String showMetricsCmd = "show metrics --categories=cache";
    ArrayList<String> cmdResultLines = runDataCommand(showMetricsCmd);
    Log.getLogWriter().info("verifyShowMetricsDataTest-Initial Show Metrics-Plain");
    verifyResultValue(showMetricsCmd, getResultValue(cmdResultLines, "totalRegionCount"), "0");
    verifyResultValue(showMetricsCmd, getResultValue(cmdResultLines, "totalRegionEntryCount"), "0");
    verifyResultValue(showMetricsCmd, getResultValue(cmdResultLines, "totalMissCount"), "0");
    int totalHitCountValue = Integer.parseInt(getResultValue(cmdResultLines, totalHitCount));
    verifyResultGreaterThan(showMetricsCmd, totalHitCountValue, 0);

    // CREATE our region and verify that it was created (must follow create region command with a sleep command #46391)
    String createRegionCmd = "create region --type=REPLICATE --name=" + regionName;
    verifyResultContains(createRegionCmd, runDataCommand(createRegionCmd), "Region \"/" + regionName + "\" created on");
    saveError(dataErrorKey,
      new StringBuilder("Bug #46391 Detected - Must issue a 'sleep' command to ensure that the region was created prior to any further operations."));
    execSleepCommad("5");

    // Check the initial values (by Region)
    String showMetricsRegionCmd = "show metrics --categories=cluster,region --region=/" + regionName;
    cmdResultLines = runDataCommand(showMetricsRegionCmd);
    Log.getLogWriter().info("verifyShowMetricsDataTest-Initial Show Metrics-Region");
    verifyResultValue(showMetricsRegionCmd, getResultValue(cmdResultLines, "member count"), "1");
    verifyResultValue(showMetricsRegionCmd, getResultValue(cmdResultLines, "region entry count"), "0");

    // Check the initial values (by Member)
    // Using DESCRIBE, save one of the member names for later
    String describeRegionCmd = "describe region --name=";
    String memberName = getResultValue(runDataCommand(describeRegionCmd + regionName), "Hosting Members", ":");
    String showMetricsMemberCmd = "show metrics --categories=region --member=" + memberName;
    cmdResultLines = runDataCommand(showMetricsMemberCmd);
    Log.getLogWriter().info("verifyShowMetricsDataTest-Initial Show Metrics-Member");
    verifyResultValue(showMetricsMemberCmd, getResultValue(cmdResultLines, "totalRegionCount"), "1");
    verifyResultValue(showMetricsMemberCmd, getResultValue(cmdResultLines, "totalRegionEntryCount"), "0");
    verifyResultValue(showMetricsMemberCmd, getResultValue(cmdResultLines, totalHitCount), "3");

    // Put some data and validate
    //String command = "put --key=key0 --value=value1 --region=/" + regionName;
    //verifyResultValue(command, getResultValue(runDataCommand(command), "Result", ":"), "true");
    for (int i = 1;i <= 10;i++) {
      runDataCommand("put --key=key" + i + " --value=value1 --region=/" + regionName);
    }
    for (int i = 5;i < 15;i++) {
      runDataCommand("get --key=key" + i + " --region=/" + regionName);
    }
    runDataCommand("remove --key=key10 --region=/" + regionName);

    //Must issue a 'sleep' command to ensure that the MBean updates are reported.
    execSleepCommad("5");

    // Check the updated values
    cmdResultLines = runDataCommand(showMetricsCmd);
    Log.getLogWriter().info("verifyShowMetricsDataTest-2nd Show Metrics-Plain");
    verifyResultValue(showMetricsCmd, getResultValue(cmdResultLines, "totalRegionCount"), "1");
    verifyResultValue(showMetricsCmd, getResultValue(cmdResultLines, "totalRegionEntryCount"), "9");
    verifyResultValue(showMetricsCmd, getResultValue(cmdResultLines, "totalMissCount"), "0");
    int oldTotalHitCountValue = totalHitCountValue;
    totalHitCountValue = Integer.parseInt(getResultValue(cmdResultLines, totalHitCount));
    verifyResultGreaterThan(showMetricsCmd, totalHitCountValue, oldTotalHitCountValue);
    cmdResultLines = runDataCommand(showMetricsRegionCmd);
    Log.getLogWriter().info("verifyShowMetricsDataTest-2nd Show Metrics-Region");
    verifyResultValue(showMetricsRegionCmd, getResultValue(cmdResultLines, "region entry count"), "9");
    // member count       | 1,         |
    // lastModifiedTime   | 0,         |
    // lastAccessedTime   | 0,
    // missCount          | 0,         |
    // hitCount           | 0,         |
    // hitRatio           | 0,         |
    // getsRate           | 0,         |
    // putsRate           | 0,         |
    // createsRate        | 0,         |
    // destroyRate        | 0,         |
    // putAllRate         | 0
    cmdResultLines = runDataCommand(showMetricsMemberCmd);
    Log.getLogWriter().info("verifyShowMetricsDataTest-2nd Show Metrics-Member");
    verifyResultValue(showMetricsMemberCmd, getResultValue(cmdResultLines, "totalRegionCount"), "1");
    verifyResultValue(showMetricsMemberCmd, getResultValue(cmdResultLines, "totalRegionEntryCount"), "9");
    verifyResultValue(showMetricsMemberCmd, getResultValue(cmdResultLines, totalHitCount), "9");

    Log.getLogWriter().info("verifyShowMetricsDataTest-End");
  }

  private void verifyCompressedDataTest() {
    Log.getLogWriter().info("verifyCompressedDataTest-Start");

    // LIST the regions to validate that our region doesn't exist
    String listRegionsCmd = CMD_LIST_REGIONS;
    verifyResultContains(listRegionsCmd, runDataCommand(listRegionsCmd), "No Regions Found");

    // CREATE both our regions (compressed and not-compressed) and verify that it was created (must follow create region command with a sleep command #46391)
    String createRegionCmd = "create region --type=REPLICATE --name=" + UNCOMP_REGION_NAME;
    verifyResultContains(createRegionCmd, runDataCommand(createRegionCmd), "Region \"/" + UNCOMP_REGION_NAME + "\" created on");

    createRegionCmd = "create region --type=REPLICATE --name=" + COMP_REGION_NAME + " --compressor=" + RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER;
    verifyResultContains(createRegionCmd, runDataCommand(createRegionCmd), "Region \"/" + COMP_REGION_NAME + "\" created on");
    //saveError(dataErrorKey,
    //  new StringBuilder("Bug #46391 Detected - Must issue a 'sleep' command to ensure that the region was created prior to any further operations."));
    execSleepCommad("5");

    // Perform a Describe Region to ensure that we have zero entries
    String describeRegionCmd = "describe region --name=" + UNCOMP_REGION_NAME;
    verifyResultValue(describeRegionCmd, getResultValue(runDataCommand(describeRegionCmd), "size"), "0");

    describeRegionCmd = "describe region --name=" + COMP_REGION_NAME;
    ArrayList<String> cmdResultLines = runDataCommand(describeRegionCmd);
    verifyResultValue(describeRegionCmd, getResultValue(cmdResultLines, "compressor"), RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);
    verifyResultValue(describeRegionCmd, getResultValue(cmdResultLines, "size"), "0");

    // Put some data
    String putCommandPre = "put --key=key1 --value=\"" + COMPRESSION_PUT_VALUE + "\" --region=/";
    String putCommand = putCommandPre + UNCOMP_REGION_NAME;
    cmdResultLines = runDataCommand(putCommand);
    // Check the results
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Old Value", ":"), "<NULL>");

    putCommand = putCommandPre + COMP_REGION_NAME;
    cmdResultLines = runDataCommand(putCommand);
    // Check the results
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(putCommand, getResultValue(cmdResultLines, "Old Value", ":"), "<NULL>");

    //Must issue a 'sleep' command to ensure that the MBean updates are reported.
    execSleepCommad("5");

    // Use GET to check to see if we have some data
    String getCommand = "get --key=key1 --region=/" + UNCOMP_REGION_NAME;
    cmdResultLines = runDataCommand(getCommand);
    verifyResultValue(getCommand, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(getCommand, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(getCommand, getResultValue(cmdResultLines, "Value  ", ":"), COMPRESSION_PUT_VALUE);

    getCommand = "get --key=key1 --region=/" + COMP_REGION_NAME;
    cmdResultLines = runDataCommand(getCommand);
    verifyResultValue(getCommand, getResultValue(cmdResultLines, "Result", ":"), "true");
    verifyResultValue(getCommand, getResultValue(cmdResultLines, "Key  ", ":"), "key1");
    verifyResultValue(getCommand, getResultValue(cmdResultLines, "Value  ", ":"), COMPRESSION_PUT_VALUE);

    Log.getLogWriter().info("verifyCompressedDataTest-End");
  }

  private String getResultValue(ArrayList<String> cmdResultLines, String searchString) {
    return getResultValue(cmdResultLines, searchString, "|");
  }
  private String getResultValue(ArrayList<String> cmdResultLines, String searchString, String delimeter) {
    //Log.getLogWriter().info("getResultValue-Start");
    //Log.getLogWriter().info("getResultValue-cmdResultLines=" + cmdResultLines);
    //Log.getLogWriter().info("getResultValue-searchString=" + searchString);
    String rtnString = new String();

    for (String line : cmdResultLines) {
      if (line.contains(searchString)) {
        int indexOfPipe = line.lastIndexOf(delimeter);
        rtnString = line.substring(indexOfPipe + 2).trim();
        break;
      }
    }

    Log.getLogWriter().info("getResultValue-End '" + searchString + "'=" + rtnString);
    return rtnString;
  }

  private void verifyResultValue(String command, String resultValue, String expectedValue) {
    verifyResultValue(command, resultValue, expectedValue, null);
  }
  private void verifyResultValue(String command, String resultValue, String expectedValue, String extraErrorText) {
    //Log.getLogWriter().info("verifyResultValue from\ncommand '" + command +
    //                        "'\nreturned '" + resultValue +
    //                        "'\nexpected '" + expectedValue + "'");
    if (!resultValue.equals(expectedValue)) {
      StringBuilder exceptionText = new StringBuilder();
      if (extraErrorText != null) {
        exceptionText.append(extraErrorText);
      }
      exceptionText.append("Command: |");
      exceptionText.append(command);
      exceptionText.append("| - Returned unexpected output: |");
      exceptionText.append(resultValue);
      exceptionText.append("| - Expected output: |");
      exceptionText.append(expectedValue).append("|");
      saveError(dataErrorKey, exceptionText);
    }
  }
  private void verifyResultGreaterThan(String command, int resultValue, int expectedValue) {
    verifyResultGreaterThan(command, resultValue, expectedValue, null);
  }
  private void verifyResultGreaterThan(String command, int resultValue, int expectedValue, String extraErrorText) {
    //Log.getLogWriter().info("verifyResultGreaterThan from\ncommand '" + command +
    //                        "'\nreturned '" + resultValue +
    //                        "'\nexpected '" + expectedValue + "'");
    if (resultValue < expectedValue) {
      StringBuilder exceptionText = new StringBuilder();
      if (extraErrorText != null) {
        exceptionText.append(extraErrorText);
      }
      exceptionText.append("Command: |");
      exceptionText.append(command);
      exceptionText.append("| - Returned unexpected output: |");
      exceptionText.append(resultValue);
      exceptionText.append("| - is less than the Expected output: |");
      exceptionText.append(expectedValue).append("|");
      saveError(dataErrorKey, exceptionText);
    }
  }
  private void verifyResultContains(String command, ArrayList<String> cmdResultLines, String searchValue) {
    verifyResultContains(command, cmdResultLines, searchValue, null);
  }
  private void verifyResultContains(String command, ArrayList<String> cmdResultLines, String searchValue, String extraErrorText) {
    boolean found = false;
    for (String line : cmdResultLines) {
      if (line.contains(searchValue)) {
        found = true;
        break;
      }
    }
    if (!found) {
      StringBuilder exceptionText = new StringBuilder();
      if (extraErrorText != null) {
        exceptionText.append(extraErrorText);
      }
      exceptionText.append("The result from the command |");
      exceptionText.append(command);
      exceptionText.append("| was missing the value |");
      exceptionText.append(searchValue).append("|");
      saveError(dataErrorKey, exceptionText);
    }
  }
  private void verifyResultNotContains(String command, ArrayList<String> cmdResultLines, String searchValue) {
    verifyResultNotContains(command, cmdResultLines, searchValue, null);
  }
  private void verifyResultNotContains(String command, ArrayList<String> cmdResultLines, String searchValue, String extraErrorText) {
    boolean found = false;
    for (String line : cmdResultLines) {
      if (line.contains(searchValue)) {
        found = true;
        break;
      }
    }
    if (found) {
      StringBuilder exceptionText = new StringBuilder();
      if (extraErrorText != null) {
        exceptionText.append(extraErrorText);
      }
      exceptionText.append("The result from the command |");
      exceptionText.append(command);
      exceptionText.append("| contained (and shouldn't) the value |");
      exceptionText.append(searchValue).append("|");
      saveError(dataErrorKey, exceptionText);
    }
  }

  private static void saveError(String errorKey, StringBuilder newError) {
    Log.getLogWriter().info("saveError-Start newError=" + newError);
    ArrayList<String> errorList = (ArrayList<String>) CommandBB.getBB().getSharedMap().get(errorKey);
    if (errorList == null) {
      errorList = new ArrayList<String>();
    }
    newError.append(" - Executed from line(s): ");
    // Build the line number trace route for the error message
    StringBuilder execLineNbrs = new StringBuilder();
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    //for (int i = 3;i < stackTrace.length;i++) {
    for (int i = stackTrace.length - 1;i >= 0;i--) {
      if (stackTrace[i].getClassName().equalsIgnoreCase("management.test.cli.CommandTest")) {
        if (execLineNbrs.length() > 0) {
          execLineNbrs.append("-->");
        }
        execLineNbrs.append(stackTrace[i].getLineNumber());
      }
    }
    newError.append(execLineNbrs.toString());
    newError.append("\n");
    errorList.add(newError.toString());
    CommandBB.getBB().getSharedMap().put(errorKey, errorList);
  }
  private static void checkForErrors(String errorKey) {
    ArrayList<String> errorList = (ArrayList<String>) CommandBB.getBB().getSharedMap().get(errorKey);
    //Log.getLogWriter().info("checkForErrors-errorList.size()=" + errorList.size());
    if (errorList != null && errorList.size() > 0) {
      errorList.add(0, "Errors (" + errorList.size() + ") were found:\n");
      throw new TestException(errorList.toString());
    } else {
      Log.getLogWriter().info("*** No Errors Were Found!  Congrats, now go celebrate! ***");
    }
  }
  private static void checkCompressedPerfStats() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      throw new TestException("CommandTest.checkPerfStats-The cache should not be null!");
    } else {
      Set<LocalRegion> theRegions = new HashSet(cache.rootRegions());
      if (theRegions.size() <= 0) {
        throw new TestException("CommandTest.checkPerfStats-We should have at least ONE region!");
      } else {
        String regionName;
        Compressor compressor;
        CachePerfStats regionPerfStats;
        long totalPreCompressedBytes;
        long totalCompressionTime;
        long totalCompressions;
        long totalPostCompressedBytes;
        long totalDecompressionTime;
        long totalDecompressions;
        for (LocalRegion aRegion : theRegions) {
          regionName = aRegion.getName();
          compressor = aRegion.getAttributes().getCompressor();
          regionPerfStats = aRegion.getRegionPerfStats();
          totalPreCompressedBytes = regionPerfStats.getTotalPreCompressedBytes();
          totalCompressionTime = regionPerfStats.getTotalCompressionTime();
          totalCompressions = regionPerfStats.getTotalCompressions();
          totalPostCompressedBytes = regionPerfStats.getTotalPostCompressedBytes();
          totalDecompressionTime = regionPerfStats.getTotalDecompressionTime();
          totalDecompressions = regionPerfStats.getTotalDecompressions();

          Log.getLogWriter().info("CommandTest.checkPerfStats regionName = " + regionName);
          Log.getLogWriter().info("  Compressor=" + (compressor == null ? null : compressor.getClass().getName()));
          Log.getLogWriter().info("  totalPreCompressedBytes   = " + totalPreCompressedBytes);
          Log.getLogWriter().info("  totalCompressionTime   = " + totalCompressionTime);
          Log.getLogWriter().info("  totalCompressions      = " + totalCompressions);
          Log.getLogWriter().info("  totalPostCompressedBytes = " + totalPostCompressedBytes);
          Log.getLogWriter().info("  totalDecompressionTime = " + totalDecompressionTime);
          Log.getLogWriter().info("  totalDecompressions    = " + totalDecompressions);

          Object theValue = aRegion.get("key1");
          if (!theValue.equals(COMPRESSION_PUT_VALUE)) {
            saveError(dataErrorKey, new StringBuilder("The value retrived from the region doesn't equal the one that was put there."));
          }
          if (regionName.equals(COMP_REGION_NAME)) {
            if (!compressor.getClass().getName().equals(RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER)) {
              saveError(dataErrorKey, new StringBuilder("The compressor for the compressed region is incorrect."));
            }
            if (totalCompressions != 1) {
              saveError(dataErrorKey, new StringBuilder("The total number of compressions for the compressed region should be one."));
            }
            if (totalPreCompressedBytes != COMPRESSION_PUT_VALUE.length()+3) {
              saveError(dataErrorKey, new StringBuilder("The total number of pre-compressed bytes for the compressed region is incorrect."));
            }
            if (totalCompressionTime <= 0) {
              saveError(dataErrorKey, new StringBuilder("The total compression time for the compressed region should be greater than zero."));
            }
            if (totalDecompressions != 1) {
              saveError(dataErrorKey, new StringBuilder("The total number of decompressions for the compressed region should be one."));
            }
            if (totalPostCompressedBytes <= 0) {
              saveError(dataErrorKey, new StringBuilder("The total number of post-compressed bytes for the compressed region should be greater than zero."));
            }
            if (totalDecompressionTime <= 0) {
              saveError(dataErrorKey, new StringBuilder("The total decompression time for the compressed region should be greater than zero."));
            }
          } else if (regionName.equals(UNCOMP_REGION_NAME)) {
            if (compressor != null) {
              saveError(dataErrorKey, new StringBuilder("The compressor for the uncompressed region is incorrect."));
            }
            if (totalCompressions != 0) {
              saveError(dataErrorKey, new StringBuilder("The total number of compressions for the uncompressed region should be zero."));
            }
            if (totalPreCompressedBytes != 0) {
              saveError(dataErrorKey, new StringBuilder("The total number of pre-compressed bytes for the uncompressed region should be zero."));
            }
            if (totalCompressionTime != 0) {
              saveError(dataErrorKey, new StringBuilder("The total compression time for the uncompressed region should be zero."));
            }
            if (totalDecompressions != 0) {
              saveError(dataErrorKey, new StringBuilder("The total number of decompressions for the uncompressed region should be zero."));
            }
            if (totalPostCompressedBytes != 0) {
              saveError(dataErrorKey, new StringBuilder("The total number of post-compressed bytes for the uncompressed region should be zero."));
            }
            if (totalDecompressionTime != 0) {
              saveError(dataErrorKey, new StringBuilder("The total decompression time for the uncompressed region should be zero."));
            }
          }
        }
      }
    }
  }

  /**
   * Method to do the work to test misc command attempts
   */
  private void basicCommandTest() {
    Log.getLogWriter().info("basicCommandTest-Start");

    shell.clearEvents();

    // Attempt to call an invalid command
    String command = "invalid";
    String expectedOutput = "Command \"" + command + "\" is not valid or is currently not available.";
    checkCommand(command, expectedOutput);

    Log.getLogWriter().info("basicCommandTest-End");
  }

  /**
   * Method to do the work to test the alter disk-store command
   */
  private void cmdAlterDiskStoreTest() {
    Log.getLogWriter().info("cmdAlterDiskStoreTest-Start");

    String diskStoreName = getAvailableDiskStoreName();
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "", diskStoreName, true, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION, "", getRandomRegion(), true, false);
    CmdOptionInfo diskDirsOption = new CmdOptionInfo(PARM_DISK_DIRS, "", ".", true, false);
    CmdOptionInfo[] options = { nameOption, regionOption, diskDirsOption };
    checkNamedParams(CMD_ALTER_DISKSTORE, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_ALTER_DISKSTORE, options);

    //TODO-MDP -- Can't get this to work right:  Could not process command due to GemFire error. The init file "./BACKUPds1.if" does not exist.
    /*
    // Test if the region option works with the leading slash ('/') and without
    CmdOptionInfo enableStatisticsOption = new CmdOptionInfo("enable-statistics", "", "false", false, true);
    checkRegionOption(CMD_ALTER_DISKSTORE, new CmdOptionInfo[] { nameOption, diskDirsOption, enableStatisticsOption },
      regionOption, false);
    checkRegionOption(CMD_ALTER_DISKSTORE, new CmdOptionInfo[] { nameOption, diskDirsOption, enableStatisticsOption },
      regionOption, true);
    */

    //TODO-MDP--FINISH THE DETAILS OF THIS TEST
    /*
       //String aDiskStoreName = "aDiskStore";
       //String aRegionName = "aRegion";
       //String aDiskDir = "aDir";
       expectedOutput = "Value is required for parameter \"disk-dirs\"";
       checkCommand(CMD_ALTER_DISKSTORE + PARM_DISKSTORE + aDiskStoreName + " \n" +
                    PARM_REGION + aRegionName + " \n" +
                    PARM_DISK_DIRS + aDiskDir, expectedOutput);

       // Attempt to call command with the name region and disk-dirs parameters but invalid values
       expectedOutput = "Could not parse command string. Please provide a relevant parameter";
       checkCommand(CMD_ALTER_DISKSTORE + PARM_DISKSTORE + aDiskStoreName + invalid + " \n" +
                    PARM_REGION + aRegionName + invalid + " \n" +
                    PARM_DISK_DIRS + aDiskDir, expectedOutput);
    */

    Log.getLogWriter().info("cmdAlterDiskStoreTest-End");
  }

  /**
   * Method to do the work to test the alter runtime command
   */
  private void cmdAlterRuntimeTest() {
    Log.getLogWriter().info("cmdAlterRuntimeTest-Start");

    // Check the command with a variety of bad options and values
    String baseError = "Following errors occurred while altering runtime config\n";
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", "", false, false);
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP,
      "No caching members for group %1 could be found.  Please verify the group and try again.", "", false, false);
    CmdOptionInfo logLevelOption = new CmdOptionInfo("log-level",
      baseError + "Unknown log-level \"%1\". Valid levels are: all finest finer fine config info warning error severe none.", "", false, false);
    CmdOptionInfo logDiskSpaceLimitOption = new CmdOptionInfo("log-disk-space-limit",
      baseError + "\"log-disk-space-limit\" value \"%1\" must be a number.", "", false, false);
    CmdOptionInfo logFileSizeLimitOption = new CmdOptionInfo("log-file-size-limit",
      baseError + "\"log-file-size-limit\" value \"%1\" must be a number.", "", false, false);
    CmdOptionInfo archiveDiskSpaceLimitOption = new CmdOptionInfo("archive-disk-space-limit",
      baseError + "\"archive-disk-space-limit\" value \"%1\" must be a number.", "", false, false);
    CmdOptionInfo archiveFileSizeLimitOption = new CmdOptionInfo("archive-file-size-limit",
      baseError + "\"archive-file-size-limit\" value \"%1\" must be a number.", "", false, false);
    //CmdOptionInfo statisticArchiveFileOption = new CmdOptionInfo("statistic-archive-file", "", "statArchFile", false, false);
    CmdOptionInfo statisticSampleRateOption = new CmdOptionInfo("statistic-sample-rate",
      baseError + "\"statistic-sample-rate\" value \"%1\" must be a number.", "", false, false);
    CmdOptionInfo enableStatisticsOption = new CmdOptionInfo("enable-statistics",
      "Value for enable-statistics must be either \"true\" or \"false\"", "", false, true);
    CmdOptionInfo[] options = { memberOption, groupOption,
      logLevelOption, logDiskSpaceLimitOption, logFileSizeLimitOption,
      archiveDiskSpaceLimitOption, archiveFileSizeLimitOption,
       statisticSampleRateOption, enableStatisticsOption};
    checkNamedParams(CMD_ALTER_RUNTIME, options);

    // Check the command with options specified on separate lines
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    memberOption.setValidValue(memberName);
    logLevelOption.setValidValue("info");
    checkMultiLineExecution(CMD_ALTER_RUNTIME, options);

    // Call the command without any parameters
    String expectedOutput = "Please provide a relevant parameter(s)";
    checkCommand(CMD_ALTER_RUNTIME, expectedOutput);

    // Setup...
    expectedOutput = "Please provide a relevant parameter(s)";

    // Call the command with the member parameter and a valid value - We need a member name
    String cmdMemberPart = PARM_PREFIX + PARM_MEMBER + "=" + memberName;
    checkCommand(CMD_ALTER_RUNTIME + cmdMemberPart, expectedOutput);

    // Call the command with the group parameter and a valid value - We need a group name
    String groupName = getAvailableGroupName();
    String cmdGroupPart = PARM_PREFIX + PARM_GROUP + "=" + groupName;
    checkCommand(CMD_ALTER_RUNTIME + cmdGroupPart, expectedOutput);

    // Test the log-level option
    String[][] expectedText = { { "Runtime configuration altered successfully for the following member(s)" }, { null } };
    String cmdLogLevelPart = " --log-level=info";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdLogLevelPart, expectedText);
    // Attempt to call command with a valid member parameter and the log-level parameter and a valid value
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdLogLevelPart, expectedText);
    // Attempt to call command with a valid group parameter and the log-level parameter and a valid value
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdLogLevelPart, expectedText);

    // Test the log-disk-space-limit option
    String cmdLogDiskSpaceLimitPart = " --log-disk-space-limit=";
    expectedOutput = baseError + "Could not set \"log-disk-space-limit\" to \"-1\" because its value can not be less than \"0\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdLogDiskSpaceLimitPart + "-1", expectedOutput);
    expectedOutput = baseError + "Could not set \"log-disk-space-limit\" to \"1,000,001\" because its value can not be greater than \"1,000,000\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdLogDiskSpaceLimitPart + "1000001", expectedOutput);
    cmdLogDiskSpaceLimitPart += "1000";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdLogDiskSpaceLimitPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdLogDiskSpaceLimitPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdLogDiskSpaceLimitPart, expectedText);

    // Test the log-file-size-limit option
    String cmdLogFileSizeLimitOptionPart = " --log-file-size-limit=";
    expectedOutput = baseError + "Could not set \"log-file-size-limit\" to \"-1\" because its value can not be less than \"0\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdLogFileSizeLimitOptionPart + "-1", expectedOutput);
    expectedOutput = baseError + "Could not set \"log-file-size-limit\" to \"1,000,001\" because its value can not be greater than \"1,000,000\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdLogFileSizeLimitOptionPart + "1000001", expectedOutput);
    cmdLogFileSizeLimitOptionPart += "1000";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdLogFileSizeLimitOptionPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdLogFileSizeLimitOptionPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdLogFileSizeLimitOptionPart, expectedText);

    // Test the archive-disk-space-limit option
    String cmdArchiveDiskSpaceLimitPart = " --archive-disk-space-limit=";
    expectedOutput = baseError + "Could not set \"archive-disk-space-limit\" to \"-1\" because its value can not be less than \"0\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdArchiveDiskSpaceLimitPart + "-1", expectedOutput);
    expectedOutput = baseError + "Could not set \"archive-disk-space-limit\" to \"1,000,001\" because its value can not be greater than \"1,000,000\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdArchiveDiskSpaceLimitPart + "1000001", expectedOutput);
    cmdArchiveDiskSpaceLimitPart += "1000";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdArchiveDiskSpaceLimitPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdArchiveDiskSpaceLimitPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdArchiveDiskSpaceLimitPart, expectedText);

    // Test the archive-file-size-limit option
    String cmdArchiveFileSizeLimitPart = " --archive-file-size-limit=";
    expectedOutput = baseError + "Could not set \"archive-file-size-limit\" to \"-1\" because its value can not be less than \"0\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdArchiveFileSizeLimitPart + "-1", expectedOutput);
    expectedOutput = baseError + "Could not set \"archive-file-size-limit\" to \"1,000,001\" because its value can not be greater than \"1,000,000\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdArchiveFileSizeLimitPart + "1000001", expectedOutput);
    cmdArchiveFileSizeLimitPart += "1000";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdArchiveFileSizeLimitPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdArchiveFileSizeLimitPart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdArchiveFileSizeLimitPart, expectedText);

    // Test the statistic-archive-file option
    String cmdStatisticArchiveFilePart = " --statistic-archive-file=statArchFile";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdStatisticArchiveFilePart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdStatisticArchiveFilePart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdStatisticArchiveFilePart, expectedText);

    // Test the statistic-sample-rate option
    String cmdStatisticSampleRatePart = " --statistic-sample-rate=";
    expectedOutput = baseError + "Could not set \"statistic-sample-rate\" to \"-1\" because its value can not be less than \"100\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdStatisticSampleRatePart + "-1", expectedOutput);
    expectedOutput = baseError + "Could not set \"statistic-sample-rate\" to \"60,001\" because its value can not be greater than \"60,000\".";
    checkCommand(CMD_ALTER_RUNTIME + cmdStatisticSampleRatePart + "60001", expectedOutput);
    cmdStatisticSampleRatePart += "1000";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdStatisticSampleRatePart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdStatisticSampleRatePart, expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdStatisticSampleRatePart, expectedText);

    // Test the enable-statistics option
    String cmdEnableStatisticsPart = " --enable-statistics=";
    checkOutputLines(CMD_ALTER_RUNTIME + cmdEnableStatisticsPart + "true", expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdEnableStatisticsPart + "false", expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdEnableStatisticsPart + "true", expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdMemberPart + cmdEnableStatisticsPart + "false", expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdEnableStatisticsPart + "true", expectedText);
    checkOutputLines(CMD_ALTER_RUNTIME + cmdGroupPart + cmdEnableStatisticsPart + "false", expectedText);

    Log.getLogWriter().info("cmdAlterRuntimeTest-End");
  }

  /**
   * Method to do the work to test the describe config command
   */
  private void cmdDescribeConfigTest() {
    Log.getLogWriter().info("cmdDescribeConfigTest-Start");

    // Check the command with a variety of bad options and values
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER, "", memberName, true, false);
    CmdOptionInfo hideDefaultsOption = new CmdOptionInfo(PARM_HIDE_DEFAULTS, "", "", false, true);
    CmdOptionInfo[] options = { memberOption, hideDefaultsOption };
    checkNamedParams(CMD_DESCRIBE_CONFIG, options);

    // Check the command with options specified on separate lines
    hideDefaultsOption.setValidValue("true");
    checkMultiLineExecution(CMD_DESCRIBE_CONFIG, options);

    //TODO-MDP--SEEMS LIKE THERE SHOULD BE A POSSITIVE TEST HERE!
    // Call the command with once for each member's name and once for each member's ID
    Map<String, String>  sharedMap = (Map<String, String>) CommandBB.getBB().getSharedMap().getMap();
    for (String key : sharedMap.keySet()) {
      Log.getLogWriter().info("cmdDescribeConfigTest-key=" + key);
      if (key.startsWith(memberNameKeyPrefix) || key.startsWith(memberIdKeyPrefix)) {
        String value = sharedMap.get(key);
        Log.getLogWriter().info("cmdDescribeConfigTest-value=" + value);
        String output = execCommand(CMD_DESCRIBE_CONFIG + PARM_PREFIX + PARM_MEMBER + "=" + value);
      }
    }

    Log.getLogWriter().info("cmdDescribeConfigTest-End");
  }

  /**
   * Method to do the work to test the list member command
   */
  private void cmdListMemberTest() {
    Log.getLogWriter().info("cmdListMemberTest-Start");
    final String msg_no_members_found = "No Members Found";

    // Check the command with a variety of bad options and values
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP, msg_no_members_found, "", false, false);
    CmdOptionInfo[] options = { groupOption };
    checkNamedParams(CMD_LIST_MEMBERS, options);

    // Check the command with options specified on separate lines
    groupOption.setValidValue(getAvailableGroupName());
    checkMultiLineExecution(CMD_LIST_MEMBERS, options);

    // Call the command with no parameters - should result in all members
    StringBuilder command = new StringBuilder(CMD_LIST_MEMBERS);
    String outputText = execCommand(command.toString(), false)[0];
    if (outputText.equals(msg_no_members_found)) {
      saveError(cmdErrorKey, formatExceptionText(command.toString(), outputText));
    }
    String saveAllOutputText = outputText;

    // Call the command with a valid group - should not result in all members
    command.append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_LOCATOR);
    outputText = execCommand(command.toString(), false)[0];
    if (outputText.equals(msg_no_members_found) ||
        outputText.equals(saveAllOutputText)) {
      saveError(cmdErrorKey, formatExceptionText(command.toString(), outputText));
    }
    String saveLocatorOutputText = outputText;

    HostHelper.OSType osType = HostHelper.getLocalHostOS();
    if (HostHelper.isWindows()) {
      saveError(cmdErrorKey, new StringBuilder("Bug #46388 Detected - command continuation character '\\' not working on windows."));
    } else {
      // Call the command with a group parameter on a new line
      command = new StringBuilder(CMD_LIST_MEMBERS).append("\\").append(System.getProperty("line.separator"));
      command.append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_LOCATOR);
      outputText = execCommand(command.toString(), false)[0];
      if (!outputText.equals(saveLocatorOutputText)) {
        saveError(cmdErrorKey, formatExceptionText(command.toString(), outputText, saveLocatorOutputText));
      }

      // Call the command with multiple group parameters
      command.append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_MEMBER);
      String expectedOutput = "Parameter group can only be specified once";
      checkCommand(command.toString(), expectedOutput);
    }

    // Call the command with a valid group - should not result in all members or the previous group
    boolean haveAdminHosts = CommandPrms.haveAdminHosts();
    String saveAdminOutputText = null;
    if (haveAdminHosts) {
      command = new StringBuilder(CMD_LIST_MEMBERS).append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_ADMIN);
      outputText = execCommand(command.toString(), false)[0];
      if (outputText.equals(msg_no_members_found) ||
          outputText.equals(saveAllOutputText) ||
          outputText.equals(saveLocatorOutputText)) {
        saveError(cmdErrorKey, formatExceptionText(command.toString(), outputText));
      }
      saveAdminOutputText = outputText;
    }

    // Call the command with a valid group - should not result in all members or the previous groups
    int nbrMemberHosts = CommandPrms.getNbrMemberHosts();
    if (nbrMemberHosts > 0) {
      command = new StringBuilder(CMD_LIST_MEMBERS).append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_MEMBER);
      outputText = execCommand(command.toString(), false)[0];
      if (outputText.equals(msg_no_members_found) ||
          outputText.equals(saveAllOutputText) ||
          outputText.equals(saveLocatorOutputText) ||
          (haveAdminHosts && outputText.equals(saveAdminOutputText))) {
        saveError(cmdErrorKey, formatExceptionText(command.toString(), outputText));
      }

      for (int i = 1;i <= nbrMemberHosts;i++) {
        command.append(i);
        outputText = execCommand(command.toString(), false)[0];
        if (outputText.equals(msg_no_members_found) ||
            outputText.equals(saveAllOutputText) ||
            outputText.equals(saveLocatorOutputText) ||
            (haveAdminHosts && outputText.equals(saveAdminOutputText))) {
          saveError(cmdErrorKey, formatExceptionText(command.toString(), outputText));
        }
      }
    }

    Log.getLogWriter().info("cmdListMemberTest-End");
  }

  /**
   * Method to do the work to test the describe member command
   */
  private void cmdDescribeMemberTest() {
    Log.getLogWriter().info("cmdDescribeMemberTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "Member \"%1\" not found", getRandomMember(), true, false);
    CmdOptionInfo[] options = { nameOption };
    checkNamedParams(CMD_DESCRIBE_MEMBER, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_DESCRIBE_MEMBER, options);

    // Call the command with once for each member's name and once for each member's ID
    String command;
    Map<String, String>  sharedMap = (Map<String, String>) CommandBB.getBB().getSharedMap().getMap();
    for (String key : sharedMap.keySet()) {
      Log.getLogWriter().info("cmdDescribeMemberTest-key=" + key);
      if (key.startsWith(memberNameKeyPrefix) || key.startsWith(memberIdKeyPrefix)) {
        String value = sharedMap.get(key);
        Log.getLogWriter().info("cmdDescribeMemberTest-value=" + value);
        command = CMD_DESCRIBE_MEMBER + PARM_PREFIX + PARM_NAME + "=" + value;
        //TODO-MDP-not checking for validity yet
        //TODO-MDP--Use checkOutputLines & checkOutputLine to check output
        String outputText = execCommand(command);
        //String vmID = "vmId" + key.substring(key.indexOf("_") + 1);
        //String memberInfo = getMemberInfo(vmID);
        //boolean isValid = new VerifyDescribeMemberOutput(memberInfo, outputText).validate();
      }
    }

    Log.getLogWriter().info("cmdDescribeMemberTest-End");
  }

  /**
   * Method to do the work to test the list region command
   */
  private void cmdListRegionTest() {
    Log.getLogWriter().info("cmdListRegionTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP,
      "No caching members for group %1 could be found.  Please verify the group and try again.", "", false, false);
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", "", false, false);
    CmdOptionInfo[] options = { groupOption, memberOption };
    checkNamedParams(CMD_LIST_REGIONS, options);

    // Check the command with options specified on separate lines
    groupOption.setValidValue(getAvailableGroupName());
    memberOption.setValidValue(parseMemberName(getMemberInfo(getRandomMember())));
    checkMultiLineExecution(CMD_LIST_REGIONS, options);

    // Call the command with no parameters - should result in all regions
    final String msg_no_regions_found = "No Regions Found";
    StringBuilder commandBuilder = new StringBuilder(CMD_LIST_REGIONS);
    String outputText = execCommand(commandBuilder.toString()).trim();
    if (outputText.equals(msg_no_regions_found)) {
      saveError(cmdErrorKey, formatExceptionText(commandBuilder.toString(), outputText));
    }
    String saveAllOutputText = outputText;

    // Call the command with a valid group - should result in all regions
    int nbrMemberHosts = CommandPrms.getNbrMemberHosts();
    if (nbrMemberHosts > 0) {
      commandBuilder.append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_MEMBER);
      checkCommand(commandBuilder.toString(), saveAllOutputText);

      for (int i = 1;i <= nbrMemberHosts;i++) {
        checkCommand(commandBuilder.toString() + i, saveAllOutputText);
      }

      // Call the command with multiple groups
      commandBuilder.append(", ").append(GROUP_MEMBER).append("1");
      outputText = execCommand(commandBuilder.toString()).trim();
      if (outputText.equals(msg_no_regions_found)) {
        saveError(cmdErrorKey, formatExceptionText(commandBuilder.toString(), outputText));
      }
    }

    Log.getLogWriter().info("cmdListRegionTest-Member");

    // Call the command with once for each member's name and once for each member's ID
    commandBuilder = new StringBuilder(CMD_LIST_REGIONS).append(PARM_PREFIX).append(PARM_MEMBER).append("=");
    String command;
    Map<String, String>  sharedMap = (Map<String, String>) CommandBB.getBB().getSharedMap().getMap();
    for (String key : sharedMap.keySet()) {
      if (key.startsWith(memberNameKeyPrefix) || key.startsWith(memberIdKeyPrefix)) {
        String value = sharedMap.get(key);
        command = commandBuilder.toString() + value;
        if (value.contains("edge")) { // edge clients not in the ds we are connected to, so they won't be found by the command
          outputText = execCommand(command, false)[0];
          if (!(outputText.startsWith("Member ")
                && (outputText.endsWith("could not be found.  Please verify the member name or ID and try again.")))) {
            saveError(cmdErrorKey, formatExceptionText(command, outputText));
          }
        } else {
          checkCommand(command, saveAllOutputText);
        }
      }
    }

    Log.getLogWriter().info("cmdListRegionTest-End");
  }

  /**
   * Method to do the work to test the describe region command
   */
  private void cmdDescribeRegionTest() {
    Log.getLogWriter().info("cmdDescribeRegionTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "Region : %1 not found", getRandomRegion(), true, false);
    CmdOptionInfo[] options = { nameOption };
    checkNamedParams(CMD_DESCRIBE_REGION, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_DESCRIBE_REGION, options);

    // Call the command once for each region's name
    String[][] expectedText;
    for (String regionName : (Set<String>) CommandBB.getBB().getSharedMap().get(regionNamesKey)) {
      expectedText = new String[][] {
        { ".", "Name", "Data Policy", "Hosting Members" },
        { ".", regionName.substring(1), null, null }
      };
      checkOutputLines(CMD_DESCRIBE_REGION + PARM_PREFIX + PARM_NAME  + "=" + regionName, expectedText);
    }

    Log.getLogWriter().info("cmdDescribeRegionTest-End");
  }

  /**
   * Method to do the work to test the list disk-store command
   */
  private void cmdListDiskStoreTest(boolean regionsAreRegistered) {
    Log.getLogWriter().info("cmdListDiskStoreTest-Start-regionsAreRegistered=" + regionsAreRegistered);

    // Check the command with a variety of bad options and values
    checkNamedParams(CMD_LIST_DISKSTORES);

    if (regionsAreRegistered) {
      // Call the command (there are no parameters or parameters)
      String[][] expectedText = {
        { "Member Name", "-" },
        { "Disk Store ID", "-" }
      };
      checkOutputLines(CMD_LIST_DISKSTORES, expectedText);
    } else {
      checkCommand(CMD_LIST_DISKSTORES, "No Disk Stores Found");
    }

    Log.getLogWriter().info("cmdListDiskStoreTest-End");
  }

  /**
   * Method to do the work to test the describe disk-store command
   */
  private void cmdDescribeDiskStoreTest() {
    Log.getLogWriter().info("cmdDescribeDiskStoreTest-Start");

    // Check the command with a variety of bad options and values
    // We need a member name
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER, "", memberName, true, false);
    // We need a disk store name
    String diskStoreName = findDiskStoreName(memberName, execCommand(CMD_LIST_DISKSTORES));
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "", diskStoreName, true, false);
    CmdOptionInfo[] options = { memberOption, nameOption };
    checkNamedParams(CMD_DESCRIBE_DISKSTORE, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_DESCRIBE_DISKSTORE, options);

    //TODO-MDP--SEEMS LIKE THERE SHOULD BE A POSSITIVE TEST HERE!

    Log.getLogWriter().info("cmdDescribeDiskStoreTest-End");
  }

  /**
   * Method to do the work to test the list function command
   */
  private void cmdListFunctionTest(boolean functionsAreRegistered) {
    Log.getLogWriter().info("cmdListFunctionTest-Start-functionsAreRegistered=" + functionsAreRegistered);

    // Check the command with a variety of bad options and values
    // don't pass in 'matches'
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP,
      "No caching members for group %1 could be found.  Please verify the group and try again.", "", false, false);
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", "", false, false);
    CmdOptionInfo[] options = { groupOption, memberOption };
    checkNamedParams(CMD_LIST_FUNCTIONS, options);

    // Check the command with options specified on separate lines
    groupOption.setValidValue(getAvailableGroupName());
    memberOption.setValidValue(parseMemberName(getMemberInfo(getRandomMember())));
    checkMultiLineExecution(CMD_LIST_FUNCTIONS, options);

    // Execute a default call to 'list functions'
    final String msg_no_functions_found = "No Functions Found";
    StringBuilder cmdBuilder = new StringBuilder(CMD_LIST_FUNCTIONS);
    String outputText = execCommand(cmdBuilder.toString(), false)[0];
    if (functionsAreRegistered && outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText));
    } else if (!functionsAreRegistered && !outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_functions_found));
    }

    // Make a call with the member parameter - one of the 3 optional parameters
    // We need a member name
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    cmdBuilder.append(PARM_PREFIX).append(PARM_MEMBER).append("=").append(memberName);
    outputText = execCommand(cmdBuilder.toString(), false)[0];
    if (functionsAreRegistered && outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(),outputText));
    } else if (!functionsAreRegistered && !outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_functions_found));
    }
    // We need another member name to test multiple members separated by commas
    String expectedOutput;
    int nbrOfMembers = FederationBlackboard.getBB().getMemberONs().size();
    Log.getLogWriter().info("cmdListFunctionTest-nbrOfMembers=" + nbrOfMembers);
    if (nbrOfMembers > 1) {
      // Make sure that the second member name is different than the first.
      String memberName2 = memberName;
      while (memberName2.equals(memberName)) {
        memberName2 = parseMemberName(getMemberInfo(getRandomMember()));
      }
      expectedOutput = "Parameter " + memberName2 + " is not applicable for list functions";
      cmdBuilder.append(", ").append(memberName2);
      checkCommand(cmdBuilder.toString(), expectedOutput);
    }

    // Make a call with the group parameter - one of the 3 optional parameters
    cmdBuilder = new StringBuilder(CMD_LIST_FUNCTIONS).append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_MEMBER);
    outputText = execCommand(cmdBuilder.toString(), false)[0];
    if (functionsAreRegistered && outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(),outputText));
    } else if (!functionsAreRegistered && !outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_functions_found));
    }

    if (CommandPrms.getNbrMemberHosts() > 0) {
      // Make a call with the group parameter with multiple groups - one of the 3 optional parameters
      cmdBuilder.append(", ").append(GROUP_MEMBER).append("1");
      outputText = execCommand(cmdBuilder.toString(), false)[0];
      if (functionsAreRegistered && outputText.equals(msg_no_functions_found)) {
        saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(),outputText));
      } else if (!functionsAreRegistered && !outputText.equals(msg_no_functions_found)) {
        saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_functions_found));
      }
    }

    // Make a call with the matches parameter - one of the 3 optional parameters
    final String matchCLI   = "cli.*";
    cmdBuilder = new StringBuilder(CMD_LIST_FUNCTIONS).append(PARM_PREFIX).append(PARM_MATCHES).append("=").append(matchCLI);
    outputText = execCommand(cmdBuilder.toString(), false)[0];
    if (functionsAreRegistered && outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(),outputText));
    } else if (!functionsAreRegistered && !outputText.equals(msg_no_functions_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_functions_found));
    }

    // Make a call with all of the 3 optional parameters
    cmdBuilder = new StringBuilder(CMD_LIST_FUNCTIONS).append(PARM_PREFIX).append(PARM_MEMBER).append("=").append(memberName);
    cmdBuilder.append(PARM_PREFIX).append(PARM_GROUP).append("=").append(GROUP_MEMBER);
    cmdBuilder.append(PARM_PREFIX).append(PARM_MATCHES).append("=").append(matchCLI);
    expectedOutput = "Please provide either \"member\" or \"group\" option.";
    checkCommand(cmdBuilder.toString(), expectedOutput);

    Log.getLogWriter().info("cmdListFunctionTest-End");
  }

  /**
   * This is called to add a simple function
   */
  private void addFunction() {
    Log.getLogWriter().info("addFunction-Start");

    Function fcn = (Function) testInstance.instantiateObject(1, "cli.DeployFcn1");
    FunctionService.registerFunction(fcn);
    Log.getLogWriter().info("addFunction-Registered Functions: " + FunctionService.getRegisteredFunctions());

    Log.getLogWriter().info("addFunction-End");
  }

  /**
   * Method to do the work to test the list index command
   */
  private void cmdListIndexTest(boolean indexesAreAdded) {
    Log.getLogWriter().info("cmdListIndexTest-Start-indexesAreAdded=" + indexesAreAdded);

    // Check the command with a variety of bad options and values
    CmdOptionInfo withStatsOption = new CmdOptionInfo(PARM_WITH_STATS, "", "", false, true);
    CmdOptionInfo[] options = { withStatsOption };
    checkNamedParams(CMD_LIST_INDEXES, options);

    // Check the command with options specified on separate lines
    withStatsOption.setValidValue("true");
    checkMultiLineExecution(CMD_LIST_INDEXES, options);

    // Execute a default call to 'list indexes'
    final String msg_no_indexes_found = "No Indexes Found";
    StringBuilder cmdBuilder = new StringBuilder(CMD_LIST_INDEXES);
    String outputText = execCommand(cmdBuilder.toString(), false)[0];
    String outputWithoutStats = outputText;
    if (indexesAreAdded && outputText.equals(msg_no_indexes_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText));
    } else if (!indexesAreAdded && !outputText.equals(msg_no_indexes_found)) {
      saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_indexes_found));
    }

    // Execute a call to 'list indexes' with the 'with-stats' parameter, shouldn't be the same as the previous call
    cmdBuilder.append(PARM_PREFIX).append(PARM_WITH_STATS);
    outputText = execCommand(cmdBuilder.toString(), false)[0];
    String outputWithStats = outputText;
    if (indexesAreAdded) {
      if (outputText.equals(msg_no_indexes_found) || outputText.equals(outputWithoutStats)) {
        saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText));
      }
    } else {
      if (!outputText.equals(msg_no_indexes_found)) {
        saveError(cmdErrorKey, formatExceptionText(cmdBuilder.toString(), outputText, msg_no_indexes_found));
      }
    }

    // Execute a call to 'list indexes' with the 'with-stats' parameter set to true, should be the same as the previous call
    String command = cmdBuilder.toString() + "=true";
    outputText = execCommand(command, false)[0];
    if (indexesAreAdded && !outputText.equals(outputWithStats)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, outputWithStats));
    } else if (!indexesAreAdded && !outputText.equals(msg_no_indexes_found)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, msg_no_indexes_found));
    }

    // Execute a call to 'list indexes' with the 'with-stats' parameter set to false, should be the same as the default call
    command = cmdBuilder.toString() + "=false";
    outputText = execCommand(command, false)[0];
    if (indexesAreAdded && !outputText.equals(outputWithoutStats)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, outputWithoutStats));
    } else if (!indexesAreAdded && !outputText.equals(msg_no_indexes_found)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, msg_no_indexes_found));
    }

    Log.getLogWriter().info("cmdListIndexTest-End");
  }

  /**
   * This is called to add a simple index
   */
  private void addIndex() {
    Log.getLogWriter().info("addIndex-Start");

    Cache cache = CacheHelper.getCache();
    if (cache != null) {
      QueryService queryService = cache.getQueryService();
      Region<Object, Object> region = cache.getRegion(getRandomRegion());
      String regionPath = region.getFullPath();
      Log.getLogWriter().info("addIndex-regionPath=" + regionPath);

      try {
        queryService.createIndex("index1", "myValue", regionPath);
      } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }

    Log.getLogWriter().info("addIndex-End");
  }

  /**
   * Method to do the work to test the create index command
   */
  private void cmdCreateIndexTest() {
    Log.getLogWriter().info("cmdCreateIndexTest-Start");

    // Check the command with a variety of bad options and values
    //TODO-MDP--REMOVE THE HARD CODING OF THE REGION
    String aRegion = getRandomRegion();  //"/persist_colAnchorPR_1";
    String indexName = "anIndex";
    indexName = indexName + aRegion.substring(1);  //add the region name to the index to try to make the index name unique
    //String aRegion = "replicate_region1";//getRandomRegion();
    //indexName += "_" + aRegion;  //add the region name to the index to try to make the index name unique
    String indexExpression = "myValue";
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "", indexName, true, false);
    CmdOptionInfo expressionOption = new CmdOptionInfo(PARM_EXPRESSION, "", indexExpression, true, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION, "", aRegion, true, false);
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));  //"peergemfire4_mali_27598";
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", memberName, false, false);
    CmdOptionInfo typeOption = new CmdOptionInfo(PARM_TYPE,
      "Invalid index type,value must be one of the following: range, key or hash.", "", false, false);
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP,
      "No caching members for group %1 could be found.  Please verify the group and try again.", getAvailableGroupName(), false, false);
    CmdOptionInfo[] options = { nameOption, expressionOption, regionOption, memberOption, typeOption, groupOption };
    checkNamedParams(CMD_CREATE_INDEX, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_CREATE_INDEX, options);

    // This is the command to use to destroy indexes that are to be created
    String cmdDestroyIndex = CMD_DESTROY_INDEX + PARM_PREFIX + PARM_NAME + "=" + indexName;
    // Clean-up after the previous testing
    execCommand(cmdDestroyIndex, false);

    // Test if the region option works with the leading slash ('/') and without
    // Try Create the index by region with a slash and then delete it
    checkRegionOption(CMD_CREATE_INDEX, new CmdOptionInfo[] { nameOption, expressionOption }, regionOption, false);
    execCommand(cmdDestroyIndex);
    // Try Create the index by region without a slash and then delete it
    checkRegionOption(CMD_CREATE_INDEX, new CmdOptionInfo[] { nameOption, expressionOption }, regionOption, true);
    execCommand(cmdDestroyIndex);

    // Call the command with the all required parameters
    StringBuilder cmdBuilder = new StringBuilder(CMD_CREATE_INDEX);
    cmdBuilder.append(PARM_PREFIX).append(PARM_NAME).append("=").append(indexName);
    cmdBuilder.append(PARM_PREFIX).append(PARM_EXPRESSION).append("=").append(indexExpression);
    cmdBuilder.append(PARM_PREFIX).append(PARM_REGION).append("=").append(aRegion);
    String command = cmdBuilder.toString();
    String[][] expectedText = {
      { "Index successfully created with following details", "Name", "Expression", "RegionPath", "Members which contain the index", "1." },
      { null, indexName, indexExpression, aRegion, null, null }
    };
    checkOutputLines(command, expectedText);
    // Clean up the index
    execCommand(cmdDestroyIndex);

    // Attempt to create the same index after delete
    checkOutputLines(command, expectedText);

    // Attempt to create the same index without delete
    String[][] failedText = {
      { "Failed to create index \"" + indexName + "\" due to following reasons", "Index \"" + indexName + "\" already exists.", "Occurred on following members", "1." },
      { null, null, null, null }
    };
    checkOutputLines(command, failedText);
    // Clean up the index
    execCommand(cmdDestroyIndex);

    Log.getLogWriter().info("cmdCreateIndexTest-Member");
    // Call the command with the member parameter and a valid value
    // We need a member name
    command = cmdBuilder + PARM_PREFIX + PARM_MEMBER + "=" + memberName;
    checkOutputLines(command, expectedText);
    // Clean up the index
    execCommand(cmdDestroyIndex);

    Log.getLogWriter().info("cmdCreateIndexTest-Type");
    // Call the command with the type parameter and a valid value
    command = cmdBuilder + PARM_PREFIX + PARM_TYPE + "=range";
    checkOutputLines(command, expectedText);
    // Clean up the index
    execCommand(cmdDestroyIndex);

    Log.getLogWriter().info("cmdCreateIndexTest-Group");
    // Call the command with the group parameter and a valid value
    command = cmdBuilder + PARM_PREFIX + PARM_GROUP + "=" + getAvailableGroupName();
    checkOutputLines(command, expectedText);
    // Clean up the index
    execCommand(cmdDestroyIndex);

    Log.getLogWriter().info("cmdCreateIndexTest-End");
  }

  /**
   * Method to do the work to test the destroy index command
   */
  private void cmdDestroyIndexTest() {
    Log.getLogWriter().info("cmdDestroyIndexTest-Start");

    // Check the command with a variety of bad options and values
    String indexName = "anIndexNameToDestroy";
    String indexExpression = "anIndexExpToDestroy";
    String aRegion = getRandomRegion();//TODO-MDP--MIGHT GET DUP GROUP FROM TEST METHOD ABOVE?
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME,
      "Index \"%1\" could not be destroyed for following reasons", indexName, false, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION,
      "Indexes could not be destroyed for following reasons", aRegion, false, false);
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", memberName, false, false);
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP,
      "No caching members for group %1 could be found.  Please verify the group and try again.", getAvailableGroupName(), false, false);
    CmdOptionInfo[] options = { nameOption, regionOption, memberOption, groupOption };
    checkNamedParams(CMD_DESTROY_INDEX, options);

    // This is the command to use to create indexes that are to be destroyed
    StringBuilder cmdCreateIndexBuilder = new StringBuilder(CMD_CREATE_INDEX);
    cmdCreateIndexBuilder.append(PARM_PREFIX).append(PARM_NAME).append("=").append(indexName);
    cmdCreateIndexBuilder.append(PARM_PREFIX).append(PARM_EXPRESSION).append("=").append(indexExpression);
    cmdCreateIndexBuilder.append(PARM_PREFIX).append(PARM_REGION).append("=").append(aRegion);

    // Check the command with options specified on separate lines
    execCommand(cmdCreateIndexBuilder.toString());
    checkMultiLineExecution(CMD_DESTROY_INDEX, options);
    // Clean up
    String outputText = execCommand(CMD_LIST_INDEXES);
    if (null != outputText && !outputText.equals("No Indexes Found")) {
      execCommand(CMD_DESTROY_INDEX + PARM_PREFIX + PARM_NAME + "=" + indexName);
    }

    // Test if the region option works with the leading slash ('/') and without
    // Create the index and try to delete it by region with a slash
    execCommand(cmdCreateIndexBuilder.toString());
    checkRegionOption(CMD_DESTROY_INDEX, regionOption, false);
    // Create the index and try to delete it by region without a slash
    execCommand(cmdCreateIndexBuilder.toString());
    checkRegionOption(CMD_DESTROY_INDEX, regionOption, true);

    // Call the command without any parameters
    String expectedOutput = "\"destroy index\" requires that one or more parameters be provided.";
    checkCommand(CMD_DESTROY_INDEX, expectedOutput);

    Log.getLogWriter().info("cmdDestroyIndexTest-Name");
    // Create a index (to destroy by name)
    execCommand(cmdCreateIndexBuilder.toString());

    // Call the command with the name parameter and bad value
    String invalid = "invalid";
    StringBuilder cmdBuilder = new StringBuilder(CMD_DESTROY_INDEX).append(PARM_PREFIX).append(PARM_NAME).append("=");
    String[][] expectedText = {
      {
        "Index \"" + invalid + "\" could not be destroyed for following reasons",
        "Index named \"" + invalid + "\" not found.",
        "Occurred on following members",
        "1."
      },
      { null, null, null, null }
    };
    checkOutputLines(cmdBuilder.toString() + invalid, expectedText);

    // Call the command with the name parameter and a valid value
    String[][] successText = {
      { "Index \"" + indexName + "\" successfully destroyed on the following members", "1." },
      { null, null }
    };
    checkOutputLines(cmdBuilder.toString() + indexName, successText);

    Log.getLogWriter().info("cmdDestroyIndexTest-Region");
    // Create a index (to destroy by region)
    execCommand(cmdCreateIndexBuilder.toString());
    // Call the command with the region parameter and bad value
    cmdBuilder = new StringBuilder(CMD_DESTROY_INDEX).append(PARM_PREFIX).append(PARM_REGION).append("=");
    expectedText = new String[][]{
      {
        "Indexes could not be destroyed for following reasons",
        "Region \"" + invalid + "\" not found.",
        "Occurred on following members",
        "1."
      },
      { null, null, null, null }
    };
    checkOutputLines(cmdBuilder.toString() + invalid, expectedText);
    // Call the command with the region parameter and a valid value
    successText[0][0] = "Indexes on region : " + aRegion + " successfully destroyed on the following members";
    checkOutputLines(cmdBuilder.toString() + aRegion, successText);

    Log.getLogWriter().info("cmdDestroyIndexTest-Member");
    // Create a index (to destroy by member)
    execCommand(cmdCreateIndexBuilder.toString() + PARM_PREFIX + PARM_MEMBER + "=" + memberName);
    // Call the command with the member parameter and a valid value
    cmdBuilder = new StringBuilder(CMD_DESTROY_INDEX).append(PARM_PREFIX).append(PARM_MEMBER).append("=");
    successText[0][0] = "Indexes successfully destroyed on the following members";
    checkOutputLines(cmdBuilder.toString() + memberName, successText);

    Log.getLogWriter().info("cmdDestroyIndexTest-Group");
    // Create a index (to destroy by group)
    String indexGroup = getAvailableGroupName();
    execCommand(cmdCreateIndexBuilder.toString() + PARM_PREFIX + PARM_GROUP + "=" + indexGroup);
    // Call the command with the group parameter and a valid value
    cmdBuilder = new StringBuilder(CMD_DESTROY_INDEX).append(PARM_PREFIX).append(PARM_GROUP).append("=");
    checkOutputLines(cmdBuilder.toString() + indexGroup, successText);

    Log.getLogWriter().info("cmdDestroyIndexTest-End");
  }

  private void destroyRegionTest() {
    //test for bug 45996/ 45910
    String region = "/persist_colAnchorPR_1";
    String expectedResult = "Error occurred while destroying region \"" + region + "\". Reason: " +
                            "The parent region [/persist_colAnchorPR_1] in colocation chain cannot be destroyed, " +
                            "unless all its children [/persist_colPR_3, /persist_colPR_4, /persist_colPR_5, /persist_colPR_6] are destroyed";
    checkCommand(CMD_DESTROY_REGION + PARM_PREFIX + PARM_NAME + "=" + region, expectedResult);
    region = "persist_colAnchorPR_1";
    expectedResult = "Error occurred while destroying region \"" + region + "\". Reason: " +
        "The parent region [/persist_colAnchorPR_1] in colocation chain cannot be destroyed, " +
        "unless all its children [/persist_colPR_3, /persist_colPR_4, /persist_colPR_5, /persist_colPR_6] are destroyed";
    checkCommand(CMD_DESTROY_REGION + PARM_PREFIX + PARM_NAME + "=" + region, expectedResult);
  }

  /**
   * Method to do the work to test the show dead-lock command
   */
  private void cmdShowDeadLockTest() {
    Log.getLogWriter().info("cmdShowDeadLockTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo fileOption = new CmdOptionInfo(PARM_FILE, "Invalid file type, the file extension must be \".txt\"", "file.txt", true, false);
    CmdOptionInfo[] options = { fileOption };
    checkNamedParams(CMD_SHOW_DEADLOCKS, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_SHOW_DEADLOCKS, options);

    // Execute a default call to 'show dead-lock' without any dead-locks
    String filename = "deadlock.txt";
    String command = CMD_SHOW_DEADLOCKS + PARM_PREFIX + PARM_FILE + "=" + filename;
    String outputText = execCommand(command);
    final String msg_no_dead_lock_found = "No dead lock detected.\n\nPlease view the dependencies between the members in file : deadlock.txt";
    if (!outputText.trim().equals(msg_no_dead_lock_found)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, msg_no_dead_lock_found));
    }
    checkFileCreation(command, filename);

    //TODO-MDP--Add a deadlock and retest

    Log.getLogWriter().info("cmdShowDeadLockTest-End");
  }

  /**
   * Method to do the work to test the show metrics command
   */
  private void cmdShowMetricsTest() {
    Log.getLogWriter().info("cmdShowMetricsTest-Start");

    // Check the command with a variety of bad options and values
    // We need a member name
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", memberName, false, false);
    CmdOptionInfo regionOption = new CmdOptionInfo(PARM_REGION,
      "Unable to retrieve metrics : Distributed Region MBean for /%1 not found", getRandomRegion(), false, false);
    CmdOptionInfo fileOption = new CmdOptionInfo(PARM_FILE,
      "Invalid file type, the file extension must be \".csv\"", "", false, false);
    CmdOptionInfo portOption = new CmdOptionInfo(PARM_PORT,
      "If the --port parameter is specified, then the --member parameter must also be specified.", "", false, false);
    CmdOptionInfo categoriesOption = new CmdOptionInfo(PARM_CATEGORIES, "Invalid Categories\n%1", "", false, false);
    CmdOptionInfo[] options = { memberOption, regionOption, fileOption, portOption, categoriesOption };
    checkNamedParams(CMD_SHOW_METRICS, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_SHOW_METRICS, options);

    // Test if the region option works with the leading slash ('/') and without
    checkRegionOption(CMD_SHOW_METRICS, regionOption, false);
    checkRegionOption(CMD_SHOW_METRICS, regionOption, true);

    // Call the command with no parameters - System Wide Metrics
    StringBuilder cmdBuilder = new StringBuilder(CMD_SHOW_METRICS);
    String expectedTitle = "Cluster-wide Metrics";
    String[][] expectedText = {
      { expectedTitle, "", "Category", "-", "cluster", "cache", "| totalRegionCount",
        "| totalMissCount", "| totalHitCount", "diskstore", "| diskReadsRate", "| diskWritesRate",
        "| flushTimeAvgLatency", "| totalBackupInProgress", "query", "| queryRequestRate" },
      { null, null, "Value", "-", null, null, null, null, null, null, null, null, null, null, null, null }
    };
    String command = cmdBuilder.toString();
    String outputText = checkOutputLines(command, expectedText);
    String[] categories = { "cluster", "cache", "diskstore", "query" };
    // Test this command with the various categories
    showMetricsCategoriesTest(categories, command, outputText, expectedTitle, null);

    // Attempt to call command with wrong extension
    Log.getLogWriter().info("cmdShowMetricsTest-Export-Report-to");
    String filename = "clusterWideMetrics.txt";
    cmdBuilder.append(PARM_PREFIX).append(PARM_FILE).append("=");
    command = cmdBuilder.toString() + filename;
    checkCommand(command, "Invalid file type, the file extension must be \".csv\"");

    // Correct the extension
    filename = "clusterWideMetrics.csv";
    command = cmdBuilder.toString() + filename;
    expectedText = new String[][] {
      { expectedTitle, "", "Category", "-", "cluster", "cache", "| totalRegionCount",
        "| totalMissCount", "| totalHitCount", "diskstore", "| diskReadsRate", "| diskWritesRate",
        "| flushTimeAvgLatency", "| totalBackupInProgress", "query", "| queryRequestRate",
        "", "Cluster wide metrics exported to" },
      { null, null, "Value", "-", null, null, null, null, null, null, null, null, null, null, null, null, null, filename + "." }
    };
    outputText = checkOutputLines(command, expectedText);
    // Check to make sure the file has been created
    checkFileCreation(command, filename);
    // Test this command with the various categories
    showMetricsCategoriesTest(categories, command, outputText, expectedTitle, filename);

    Log.getLogWriter().info("cmdShowMetricsTest-Member");
    cmdBuilder = new StringBuilder(CMD_SHOW_METRICS).append(PARM_PREFIX).append(PARM_MEMBER).append("=");
    expectedTitle = "Member Metrics";
    expectedText = new String[][] {
      { expectedTitle, "", "Category", "-", "member" },
      { null, null, "Value", "-", null }
    };
    categories = new String[] { "communication", "diskstore", "distribution", "eviction", "function", "jvm", "lock", "member", "region", "serialization", "transaction" };
    // Call the command with once for each member's name and once for each member's ID
    String memberOne = null;
    String memberTwo = null;
    Map<String, String>  sharedMap = (Map<String, String>) CommandBB.getBB().getSharedMap().getMap();
    for (String key : sharedMap.keySet()) {
      if (key.startsWith(memberNameKeyPrefix) || key.startsWith(memberIdKeyPrefix)) {
        String value = sharedMap.get(key);
        command = cmdBuilder.toString() + value;
        outputText = checkOutputLines(command, expectedText);
        // Test this command with the various categories
        showMetricsCategoriesTest(categories, command, outputText, expectedTitle, filename);
        // Save some info from the the first couple of members to be used later in the test when testing the multiple members
        if (key.startsWith(memberNameKeyPrefix)) {
          if (memberOne == null) {
            memberOne = value;
          } else if (memberTwo == null) {
            memberTwo = value;
          }
        }
      }
    }

    // Call the command with once for each region's name
    Log.getLogWriter().info("cmdShowMetricsTest-Region");
    cmdBuilder = new StringBuilder(CMD_SHOW_METRICS).append(PARM_PREFIX).append(PARM_REGION).append("=");
    expectedTitle = "Cluster-wide Region Metrics";
    expectedText = new String[][] {
      { expectedTitle, "", "Category", "-", "cluster" },
      { null, null, "Value", "-", null }
    };
    categories = new String[] { "callback", "cluster", "diskstore", "eviction", "partition", "region" };
    String regionOne = null;
    String regionTwo = null;
    for (String key : (Set<String>) CommandBB.getBB().getSharedMap().get(regionNamesKey)) {
      command = cmdBuilder.toString() + key;
      outputText = checkOutputLines(command, expectedText);
      // Test this command with the various categories
      showMetricsCategoriesTest(categories, command, outputText, expectedTitle, filename);

      // Save some info from the the first couple of members to be used later in the test when testing the multiple members
      if (regionOne == null) {
        regionOne = key;
      } else if (regionTwo == null) {
        regionTwo = key;
      }
    }

    Log.getLogWriter().info("cmdShowMetricsTest-Member And Region");
    cmdBuilder = new StringBuilder(CMD_SHOW_METRICS).append(PARM_PREFIX).append(PARM_REGION).append("=").append(regionOne);
    cmdBuilder.append(PARM_PREFIX).append(PARM_MEMBER).append("=").append(memberOne);
    expectedTitle = "Metrics for region:" + regionOne + " On Member " + memberOne;
    expectedText = new String[][] {
      { expectedTitle, "", "Category", "-", "region" },
      { null, null, "Value", "-", null }
    };
    categories = new String[] { "callback", "eviction", "diskstore", "partition", "region" };
    // Call the command with a member and a region
    command = cmdBuilder.toString();
    outputText = checkOutputLines(command, expectedText);
    // Test this command with the various categories
    showMetricsCategoriesTest(categories, command, outputText, expectedTitle, filename);

    /*
    Log.getLogWriter().info("cmdShowMetricsTest-Port");
    // Attempt to call command with missing port parameter - should not work, but we don't want to fail the test
    expectedOutput = "Value is required for parameter \"port\"";
    checkCommand(CMD_SHOW_METRICS + PARM_PREFIX + PARM_PORT, expectedOutput);

    // Call the command with a port without the required 'member' parameter
    expectedOutput = "If the --port parameter is specified, then the --member parameter must also be specified.";
    checkCommand(CMD_SHOW_METRICS + PARM_PREFIX + PARM_PORT + "=" + invalid, expectedOutput);

    // Call the command with an invalid port with the required 'member' parameter
    expectedOutput = "Invalid port";
    checkCommand(CMD_SHOW_METRICS + PARM_PREFIX + PARM_MEMBER + "=" + memberOne + PARM_PREFIX + PARM_PORT + "=" + invalid, expectedOutput);

    // Call the command with an incorrect port with the required 'member' parameter
    expectedOutput = "Metrics for the Cache Server with port : 12,345 and member : " + memberOne + " not found.\n"
                     + " Please check the port number and the member name/id";
    checkCommand(CMD_SHOW_METRICS + PARM_PREFIX + PARM_MEMBER + "=" + memberOne + PARM_PREFIX + PARM_PORT + "=12345", expectedOutput);
    */

    //TODO-MDP--FINISH THIS TEST BY CALLING THE COMMAND WITH A VALID PORT

    Log.getLogWriter().info("cmdShowMetricsTest-End");
  }

  private void showMetricsCategoriesTest(String[] categories, String baseCommand, String outputText, String expectedTitle, String filename) {
    Log.getLogWriter().info("showMetricsCategoriesTest-Start");

    //Check for the right categories in the output text
    for (String aCategory : categories) {
      if (!outputText.contains(aCategory)) {
        StringBuilder error = new StringBuilder("Command '").append(baseCommand).append("' returned unexpected output:'");
        error.append(outputText).append("'.  Expected output should contain:'").append(aCategory).append("'.");
        saveError(cmdErrorKey, error);
      }
    }

    // Test the command with a single category
    baseCommand = baseCommand + PARM_PREFIX + PARM_CATEGORIES + "=";
    String command;
    String multipleCategories = new String();
    for (String aCategory : categories) {
      multipleCategories += aCategory + ",";
      command = baseCommand + aCategory;
      outputText = execCommand(command, false)[0];
      if (!outputText.startsWith(expectedTitle)) {
        StringBuilder error = new StringBuilder("Command '").append(command).append("' returned unexpected output:'");
        error.append(outputText).append("'.  Expected output should start with:'").append(expectedTitle).append("'.");
        saveError(cmdErrorKey, error);
      } else if (!outputText.contains(aCategory)) {
        StringBuilder error = new StringBuilder("Command '").append(command).append("' returned unexpected output:'");
        error.append(outputText).append("'.  Expected output should contain:'").append(aCategory).append("'.");
        saveError(cmdErrorKey, error);
      }
      if (null != filename) {
        checkFileCreation(command, filename);
      }
    }
    // Test the command with multiple categories
    command = baseCommand + multipleCategories;
    outputText = execCommand(command, false)[0];
    if (!outputText.startsWith(expectedTitle)) {
      StringBuilder error = new StringBuilder("Command '").append(command).append("' returned unexpected output:'");
      error.append(outputText).append("'.  Expected output should start with:'").append(expectedTitle).append("'.");
      saveError(cmdErrorKey, error);
    } else {
      //check to make sure the last command output contains all the categories
      for (String aCategory : categories) {
        if (!outputText.contains(aCategory)) {
          StringBuilder error = new StringBuilder("Command '").append(command).append("' returned unexpected output:'");
          error.append(outputText).append("'.  Expected output should contain:'").append(aCategory).append("'.");
          saveError(cmdErrorKey, error);
        }
      }
    }
    if (null != filename) {
      checkFileCreation(command, filename);
    }

    Log.getLogWriter().info("showMetricsCategoriesTest-End");
  }

  /**
   * Method to do the work to test the export stacktrace command
   */
  private void cmdExportStackTraceTest() {
    Log.getLogWriter().info("cmdExportStackTraceTest-Start");

    // Check the command with a variety of bad options and values
    String filename = "stacktrace.txt";
    CmdOptionInfo fileOption = new CmdOptionInfo(PARM_FILE, "", filename, true, false);
    // We need a member name
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER,
      "Member %1 could not be found.  Please verify the member name or ID and try again.", memberName, false, false);
    String groupName = getAvailableGroupName();
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP,
      "No caching members for group %1 could be found.  Please verify the group and try again.", groupName, false, false);
    CmdOptionInfo[] options = { fileOption, memberOption, groupOption };
    checkNamedParams(CMD_EXPORT_STACKTRACES, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_EXPORT_STACKTRACES, options);

    // Attempt to call command with the required argument
    StringBuilder commandBuilder = new StringBuilder(CMD_EXPORT_STACKTRACES).append(PARM_PREFIX).append(PARM_FILE).append("=");
    String command = commandBuilder.toString() + filename;
    String[][] expectedText = {
      { "stack-trace(s) exported to file: ", "On host :" },
      { filename, null }
    };
    checkOutputLines(command, expectedText);
    checkFileCreation(command, filename);

    // Attempt to call command with the required argument and a valid value for the member parameter
    filename = "stacktrace_" + memberName + ".txt";
    command = commandBuilder.toString() + filename + PARM_PREFIX + PARM_MEMBER + "=" + memberName;
    expectedText[1][0] = filename;
    checkOutputLines(command, expectedText);
    checkFileCreation(command, filename);

    // Attempt to call command with the required argument and a valid value for the group parameter
    filename = "stacktrace_" + groupName + ".txt";
    command = commandBuilder.toString() + filename + PARM_PREFIX + PARM_GROUP + "=" + groupName;
    expectedText[1][0] = filename;
    checkOutputLines(command, expectedText);
    checkFileCreation(command, filename);

    // Attempt to call command with the required argument and a valid value for the member parameter and the group parameter
    filename = "stacktrace_" + memberName + "_" + groupName + ".txt";
    command = commandBuilder.toString() + filename + PARM_PREFIX + PARM_MEMBER + "=" + memberName + PARM_PREFIX + PARM_GROUP + "=" + groupName;
    String expectedOutput = "Please provide either \"member\" or \"group\" option.";
    checkCommand(command, expectedOutput);

    Log.getLogWriter().info("cmdExportStackTraceTest-End");
  }

  /**
   * Method to do the work to test the export config command
   */
  private void cmdExportConfigTest() {
    Log.getLogWriter().info("cmdExportConfigTest-Start");

    //TODO-MDP--this command doesn't seem to be finished

    // Check the command with a variety of bad options and values
    // We need a member name
    //String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    CmdOptionInfo memberOption = new CmdOptionInfo(PARM_MEMBER, "", "", false, false);
    CmdOptionInfo groupOption = new CmdOptionInfo(PARM_GROUP, "", "", false, false);
    CmdOptionInfo dirOption = new CmdOptionInfo(PARM_DIR, "", "", false, false);
    CmdOptionInfo[] options = { memberOption, groupOption, dirOption };
    checkNamedParams(CMD_EXPORT_CONFIG, options);

    // Check the command with options specified on separate lines
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    memberOption.setValidValue(memberName);
    dirOption.setValidValue(".");
    checkMultiLineExecution(CMD_EXPORT_CONFIG, options);

    /*
    // Attempt to call command with a bad parameter
    String command = CMD_EXPORT_CONFIG + PARM_TYPE;
    String expectedOutput = "Parameter type is not applicable for export config";
    checkCommand(command, expectedOutput);

    // Call the command without any parameters
    command = CMD_EXPORT_CONFIG;
    expectedOutput = "Files are saved.";
    String outputText = execCommand(command).trim();
    if (!outputText.equals(expectedOutput)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, expectedOutput));
    }
    // Get each member name and check if the file got created
    Map<String, String>  sharedMap = (Map<String, String>) CommandBB.getBB().getSharedMap().getMap();
    for (String key : sharedMap.keySet()) {
      Log.getLogWriter().info("cmdExportConfigTest-key=" + key);
      if (key.startsWith(memberNameKeyPrefix)) {
        String memberName = sharedMap.get(key);
        Log.getLogWriter().info("cmdExportConfigTest-value=" + memberName);
        checkFileCreation(command, memberName + "-cache.xml");
        checkFileCreation(command, memberName + "-gf.properties");
      }
    }

    Log.getLogWriter().info("cmdExportConfigTest-Member");
    // Attempt to call command with the member parameter and no value
    command = CMD_EXPORT_CONFIG + PARM_MEMBER;
    expectedOutput = "Value is required for parameter \"member\"";
    checkCommand(command, expectedOutput);

    // Attempt to call command with the member parameter and bad value
    String invalid = "invalid";
    expectedOutput = "Member " + invalid + " could not be found.  Please verify the member name or ID and try again.";
    command = CMD_EXPORT_CONFIG + PARM_MEMBER + "=" + invalid;
    checkCommand(command, expectedOutput);
    */

    //TODO-MDP-MORE TO TEST HERE
    /*
    // Call the command with the member parameter and a valid value
    // We need a member name
    String rndmMemberName = parseMemberName(getMemberInfo(getRandomMember()));
    command = CMD_EXPORT_CONFIG + PARM_MEMBER + "=" + rndmMemberName;
    outputText = execCommand(command).trim();
    checkFileCreation(command, rndmMemberName + "-cache.xml");
    checkFileCreation(command, rndmMemberName + "-gf.properties");

    Log.getLogWriter().info("cmdExportConfigTest-group");
    // Attempt to call command with the group parameter and no value
    command = CMD_EXPORT_CONFIG + PARM_GROUP;
    expectedOutput = "Value is required for parameter \"group\"";
    checkCommand(command, expectedOutput);

    // Attempt to call command with the group parameter and bad value
    expectedOutput = "No caching members for group " + invalid + " could be found.  Please verify the group and try again.";
    command = CMD_EXPORT_CONFIG + PARM_GROUP + "=" + invalid;
    checkCommand(command, expectedOutput);

    // Call the command with the group parameter and a valid value
    String groupName = "memberGroup";
    command = CMD_EXPORT_CONFIG + PARM_GROUP + "=" + groupName;
    outputText = execCommand(command).trim();
    // Get each member name and check if the file got created
    for (String key : sharedMap.keySet()) {
      Log.getLogWriter().info("cmdExportConfigTest-key=" + key);
      if (key.startsWith(memberNameKeyPrefix)) {
        String memberName = sharedMap.get(key);
        Log.getLogWriter().info("cmdExportConfigTest-value=" + memberName);
        checkFileCreation(command, memberName + "-cache.xml");
        checkFileCreation(command, memberName + "-gf.properties");
      }
    }

    Log.getLogWriter().info("cmdExportConfigTest-dir");
    // Attempt to call command with the group parameter and no value
    command = CMD_EXPORT_CONFIG + PARM_DIR;
    expectedOutput = "Value is required for parameter \"dir\"";
    checkCommand(command, expectedOutput);
    */

    Log.getLogWriter().info("cmdExportConfigTest-End");
  }

  /**
   * Method to do the work to test the status locator command
   */
  private void cmdStatusLocatorTest() {
    Log.getLogWriter().info("cmdStatusLocatorTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "No Locator with member name or ID %1 could be found.", "", false, false);
    CmdOptionInfo hostOption = new CmdOptionInfo(PARM_HOST, "The hostname/IP address to which the Locator will be bound is unknown.", "", false, false);
    CmdOptionInfo portOption = new CmdOptionInfo(PARM_PORT, "Value \"%1\" is not applicable for \"port\".", "", false, false);
    CmdOptionInfo pidOption = new CmdOptionInfo(PARM_PID, "Value \"%1\" is not applicable for \"pid\".", "", false, false);
    CmdOptionInfo dirOption = new CmdOptionInfo(PARM_DIR, "The working directory for the Locator could not be found.", "", false, false);
    CmdOptionInfo[] options = { nameOption, hostOption, portOption, pidOption, dirOption };
    checkNamedParams(CMD_STATUS_LOCATOR, options);

    // Check the command with options specified on separate lines
    // get the locator info
    Integer vmId = getRandomLocatorVmId();
    DistributedSystemHelper.Endpoint dsEndpoint = getLocatorInfo(vmId);
    // get the locator host info
    String locatorHost = dsEndpoint.getHost();
    hostOption.setValidValue(locatorHost);
    // get the locator port info
    String locatorPort = new Integer(dsEndpoint.getPort()).toString();
    portOption.setValidValue(locatorPort);
    options = new CmdOptionInfo[] { hostOption, portOption };
    checkMultiLineExecution(CMD_STATUS_LOCATOR, options);

    // Call the command without any options (it's 'not responding' because hydra is not using the default port)
    String[][] expectedText = { { "Locator in" }, { "is currently not responding." } };
    checkOutputLines(CMD_STATUS_LOCATOR, expectedText);

    //TODO-MDP-Gfsh must be connected in order to get the status of a Locator by member name or ID.
    // Call the command with the name option (it's 'not found' because the PID file is not created when using Hydra)
    String locatorName = dsEndpoint.getName();
    expectedText = new String[][] { { "No Locator with member name or ID " + locatorName}, { "could be found." } };
    checkOutputLines(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_NAME + "=" + locatorName, expectedText);

    // Call the command with the host option (it's 'not responding' because using just the host option only works if the port is the default (it's not in this test))
    expectedText = new String[][] { { "Locator in" }, { "is currently not responding." } };
    checkOutputLines(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_HOST + "=" + locatorHost, expectedText);

    // Call the command with the host and port options
    expectedText = new String[][] { { "Locator in" }, { "is currently online." } };
    checkOutputLines(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_PORT + "=" + locatorPort, expectedText);
    checkOutputLines(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_HOST + "=" + locatorHost + PARM_PREFIX + PARM_PORT + "=" + locatorPort, expectedText);

    // The following options (pid & dir) don't work because a PID file does not exist when starting a locater in hydra!
    /*
    // We need to 'run' the command to get the 'PID'
    ArrayList<String> cmdResultLines = runDataCommand(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_PORT + "=" + locatorPort);
    String pid = getResultValue(cmdResultLines, "Process ID", ":");
    Log.getLogWriter().info("cmdStatusLocatorTest-pid=" + pid);
    //Actual Error:  An error occurred while attempting to determine the state of Locator on mali.gemstone.com[10334] running in /home/mpriest/gfshTesting/sleep: null
    expectedText = new String[][] { { "An error occurred while attempting to determine the state of Locator on" } };
    checkOutputLines(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_PID + "=" + pid, expectedText);

    checkOutputLines(CMD_STATUS_LOCATOR + PARM_PREFIX + PARM_DIR + "=" + ???, expectedText);
    */

    Log.getLogWriter().info("cmdStatusLocatorTest-End");
  }

  /**
   * Method to do the work to test the status server command
   */
  private void cmdStatusServerTest() {
    Log.getLogWriter().info("cmdStatusServerTest-Start");

    // Check the command with a variety of bad options and values
    CmdOptionInfo nameOption = new CmdOptionInfo(PARM_NAME, "No GemFire Cache Server with member name or ID %1 could be found.", "", false, false);
    CmdOptionInfo pidOption = new CmdOptionInfo(PARM_PID, "Value \"%1\" is not applicable for \"pid\".", "", false, false);
    CmdOptionInfo dirOption = new CmdOptionInfo(PARM_DIR, "The working directory for the Server could not be found.", "", false, false);
    CmdOptionInfo[] options = { nameOption, pidOption, dirOption };
    checkNamedParams(CMD_STATUS_SERVER, options);

    // Check the command with options specified on separate lines
    checkMultiLineExecution(CMD_STATUS_SERVER, options);

    // Call the command without any options (it's 'not responding' because ...)
    String[][] expectedText = { { "Server in" }, { "is currently not responding." } };
    checkOutputLines(CMD_STATUS_SERVER, expectedText);

    // Call the command with the name option (it's 'not found' because the PID file is not created when using Hydra)
    String member = getRandomMember();
    expectedText = new String[][] { { "No GemFire Cache Server with member name or ID " + member}, { "could be found." } };
    checkOutputLines(CMD_STATUS_SERVER + PARM_PREFIX + PARM_NAME + "=" + member, expectedText);

    // Need to get the PID and DIR (currently command doesn't execute correctly from within Hydra)
    /*
    // Call the command with the name option (it's 'not found' because the PID file is not created when using Hydra)
    checkOutputLines(CMD_STATUS_SERVER + PARM_PREFIX + PARM_PID + "=" + pid, expectedText);

    // Call the command with the name option (it's 'not found' because the PID file is not created when using Hydra)
    checkOutputLines(CMD_STATUS_SERVER + PARM_PREFIX + PARM_DIR + "=.", expectedText);
    */

    Log.getLogWriter().info("cmdStatusServerTest-End");
  }

  /**
   * Method to do the work to test the describe connection command
   */
  private void cmdDescribeConnectionTest() {
    Log.getLogWriter().info("cmdDescribeConnectionTest-Start");

    // Check the command with a variety of bad options and values
    checkNamedParams(CMD_DESCRIBE_CONNECTION);

    String[][] expectedText = { { "Connection", "-" }, { "Endpoints", "-", "]" } };
    checkOutputLines(CMD_DESCRIBE_CONNECTION, expectedText);

    Log.getLogWriter().info("cmdDescribeConnectionTest-End");
  }

  /** Return a random group name defined in any member in this test
   *
   * @return The name of a group
   */
  private String getAvailableGroupName() {
    if (definedGroups == null) {
      definedGroups = new ArrayList<String>();
      synchronized(this) {
        Set<String> groupSet = new HashSet<String>();
        Map sharedMap = CommandBB.getBB().getSharedMap().getMap();
        for (Object key: sharedMap.keySet()) {
          if ((key instanceof String) && ((String)key).startsWith(groupsKeyPrefix)) {
            groupSet.addAll((List<String>)(sharedMap.get(key)));
          }
        }
        definedGroups.addAll(groupSet);
        Log.getLogWriter().info("definedGroups initialized to: " + definedGroups);
      }
    }
    if (definedGroups.size() > 0) {
      return definedGroups.get(TestConfig.tab().getRandGen().nextInt(0, definedGroups.size()-1));
    } else {
      return null;
    }
  }

  /** Randomly choose a managing member. The result can be used to connect gfsh.
   *
   * @return A String suitable to use as an argument to connectToManagingNode(String)
   */
  private String getRandomManagingMember() {
    Map managersMap = (Map) FederationBlackboard.getBB().getSharedMap().get("MANAGERS");
    if ((managersMap == null) || (managersMap.size() == 0)) {
      throw new TestException("Test problem; MANAGERS key in blackboard has value " + managersMap);
    }
    List managingMembers = new ArrayList();
    managingMembers.addAll(managersMap.keySet());
    int randInt = TestConfig.tab().getRandGen().nextInt(0, managingMembers.size() - 1);
    String chosen = (String) managingMembers.get(randInt);
    Log.getLogWriter().info("Randomly chose managing member " + chosen + " " + managersMap.get(chosen));
    return chosen;
  }

  /** Randomly choose a member. The result can be used in gfsh commands.
   *
   * @return A String suitable to use as an argument in executing gfsh commands(String)
   */
  private String getRandomMember() {
    Map memberMap = FederationBlackboard.getBB().getMemberONs();
    if ((memberMap == null) || (memberMap.size() == 0)) {
      throw new TestException("Test problem; MEMBERS vmID in blackboard has value " + memberMap);
    }
    List members = new ArrayList();
    members.addAll(memberMap.keySet());
    int randInt = TestConfig.tab().getRandGen().nextInt(0, members.size() - 1);
    String chosen = (String) members.get(randInt);
    Log.getLogWriter().info("Randomly chose member " + chosen + " " + memberMap.get(chosen));
    return chosen;
  }

  /** Randomly choose a region from the BB. The result can be used in gfsh commands.
   *
   * @return A String suitable to use as an argument in executing gfsh commands(String)
   */
  private String getRandomRegion() {
    Set<String> allRegions = (Set<String>) CommandBB.getBB().getSharedMap().get(regionNamesKey);
    if ((allRegions == null) || (allRegions.size() == 0)) {
      throw new TestException("Test problem; There are not 'regions' in the blackboard. " + allRegions);
    }
    List regionList = new ArrayList();
    regionList.addAll(allRegions);
    int randInt = TestConfig.tab().getRandGen().nextInt(0, regionList.size() - 1);
    String chosen = (String) regionList.get(randInt);
    Log.getLogWriter().info("Randomly chose region '" + chosen + "'.");
    return chosen;
  }

  private Integer getRandomLocatorVmId() {
    Log.getLogWriter().info("getRandomLocatorVmId-Start");
    Integer vmId;

    Map<Integer, String> locatorVmIdMap = (Map<Integer, String>) CommandBB.getBB().getSharedMap().get(locatorVmIdsKey);
    if (locatorVmIdMap == null) {
      throw new TestException("Test problem; There are not 'locators' in the blackboard. " + locatorVmIdMap);
    }
    List<Integer> vmIds = new ArrayList<Integer>();
    for (Integer key: locatorVmIdMap.keySet()) {
      vmIds.add(key);
    }

    // randomly choose a locator if more than one
    int index = TestConfig.tab().getRandGen().nextInt(0, vmIds.size() - 1);
    vmId = vmIds.get(index);
    Log.getLogWriter().info("getRandomLocatorVmId-End-vmId=" + vmId);

    return vmId;
  }

  private DistributedSystemHelper.Endpoint getLocatorInfo(Integer vmId) {
    Log.getLogWriter().info("getLocatorInfo-Start");
    DistributedSystemHelper.Endpoint dsEndpoint;

    // get the correct command parameter and port
    Object value = DistributedSystemBlackboard.getInstance().getSharedMap().get(vmId);
    Log.getLogWriter().info("getLocatorInfo-value=" + value);
    if ((value == null) || (!(value instanceof DistributedSystemHelper.Endpoint))) {
      throw new TestException("Could not find endpoint for vmId " + vmId + " in " + DistributedSystemBlackboard.class.getName() +
                              ", value in bb is " + ((value == null) ? "null" : (value.getClass().getName())));
    }
    dsEndpoint = (DistributedSystemHelper.Endpoint) value;
    return dsEndpoint;
  }

  /** Get the Member Information from the BB.
   *
   * @param vmID A String of the form "vmIdN" where N is a hydra vm ID.
   * @return A String suitable to use as an argument to connectToManagingNode(String)
   */
  private String getMemberInfo(String vmID) {
    Log.getLogWriter().info("getMemberInfo: vmID=|" + vmID + "|");
    Map memberMap = FederationBlackboard.getBB().getMemberONs();
    if ((memberMap == null) || (memberMap.size() == 0)) {
      throw new TestException("Test problem; MEMBERS vmID in blackboard has value " + memberMap);
    }
    // remove the unneeded start of the member info string
    //String memberInfo = ((String) memberMap.get(vmID)).substring("GemFire:type=Member,member=".length());
    String memberInfo = (String) memberMap.get(vmID);
    Log.getLogWriter().info("Member Info for " + vmID + "=" + memberInfo);
    return memberInfo;
  }

  /**
   *
   * @param memberInfo
   * @return
   */
  private String parseMemberName(String memberInfo) {
    String rtnStr;
    Log.getLogWriter().info("parseMemberName: memberInfo=|" + memberInfo + "|");

    //Parse-out the Member Name (might contain dashes)
    int expStart = memberInfo.lastIndexOf("=");
    if (expStart < 0) {
      throw new TestException("Unexpected format of memberInfo line: " + memberInfo);
    }

    rtnStr = memberInfo.substring(expStart + 1).trim();
    Log.getLogWriter().info("parseMemberName: rtnStr=|" + rtnStr + "|");
    return rtnStr;
  }

  /**
   * Use this method to find and return the name of the 1st occurrence of a disk-store name inside the output
   *  of a 'list disk-store' command.  Given the following text, this method would return 'ds1'.
   *       Member Name       |                   Member Id                   | Disk Store Name |            Disk Store ID
   * ----------------------- | --------------------------------------------- | --------------- | ------------------------------------
   * peergemfire1_mali_10527 | mali(peergemfire1_mali_10527:10527)<v1>:29834 | ds1             | 60aaadcc-7a0e-4d4e-9f43-554e1f0accf2
   *
   *
   */
  private String findDiskStoreName(String mbrName, String listDiskStoreOutput) {
    Log.getLogWriter().info("findDiskStoreName-Start-mbrName='" + mbrName +
                            "', listDiskStoreOutput='" + listDiskStoreOutput + "'");

    // this finds the start of the first line
    int start = listDiskStoreOutput.indexOf(mbrName);
    // this finds the 'pipe', just be for the ds text (it first finds (& skips) the first 'pipe')
    start = listDiskStoreOutput.indexOf("|", (listDiskStoreOutput.indexOf("|", start + 1)) + 1);
    // this finds the end of the next 'ds'
    String diskStoreName = "ds";
    start = listDiskStoreOutput.indexOf(diskStoreName, start + 1) + diskStoreName.length();
    // this finds the 'space' at the end of the disk-store name
    int end = listDiskStoreOutput.indexOf(" ", start + 1);

    // get the distinct part of the disk-store name
    diskStoreName += listDiskStoreOutput.substring(start, end);
    Log.getLogWriter().info("findDiskStoreName-End-diskStoreName='" + diskStoreName + "'");
    return diskStoreName;
  }

  /** Run a gfsh command to connect to the given member
   *
   * @param vmID A String of the form "vmIdN" where N is a hydra vm ID.
   */
  private void connectToManagingNode(String vmID) {
    Log.getLogWriter().info("connectToManagingNode: vmID=" + vmID);
    String url = FederationBlackboard.getBB().getManagingNode(vmID);
    Pattern MY_PATTERN = Pattern.compile("service:jmx:rmi:///jndi/rmi://(.*?):(.*?)/jmxrmi");
    Matcher m = MY_PATTERN.matcher(url);
    //int port=0;
    String endpoint= null;
    while (m.find()) {
      String s = m.group(1);
      String s2 = m.group(2);
      endpoint = s +"[" + s2 +"]";
    }

    Log.getLogWriter().info("Connecting shell " + shell + " to managing node hosted at url " + url );
    execCommand("connect " + "--jmx-manager=" + endpoint);
    boolean connected = shell.isConnectedAndReady();
    if (!connected) {
      throw new TestException("connect command failed to connect to manager " + endpoint);
    }
    Log.getLogWriter().info("Successfully connected to managing node ");
    String searchStr = "vmId";
    shellManagerMemberVmId = Integer.valueOf(vmID.substring(vmID.indexOf(searchStr) + searchStr.length()));
  }

  private void checkCommand(String command, String expectedOutput) {
    String outputText = execCommand(command, false)[0];
    if (!outputText.equals(expectedOutput)) {
      saveError(cmdErrorKey, formatExceptionText(command, outputText, expectedOutput));
    }
  }

  private void checkCommand(String command, CommandOutputValidator validator) {
    try {
      Map<String, Object> commandOutput = executeCommand(command);
      if (!validator.validate(commandOutput)) {
        Log.getLogWriter().info("Error while validating command output for command " + command + " : " + validator.getFormattedErrors());
        saveError(cmdErrorKey, new StringBuilder(validator.getFormattedErrors()));
      }

    } finally {
      // clear for next command
      Log.getLogWriter().info("Calling clearEvents() on " + shell);
      shell.clearEvents();
    }

  }
  

  private Map<String, Object> executeCommand(String command) {
    // run the command
    long duration = runCommand(command);
    // log the results of the command
    Log.getLogWriter().info(
        "execCommand-shell.getCompletorOutput() for " + command + "="
            + shell.getCompletorOutput());
    Map<String, Object> commandOutput = shell.getCommandOutput();
    Log.getLogWriter().info(
        "execCommand-commandOutput for " + command + "=" + commandOutput);
    return commandOutput;
  }
  
  /**
   *
   * @param command - the command to test
   * @param expectedText - in the format of [0]=expected 'starts with', [1]=expected 'ends with'
   * @return
   */
  private String checkOutputLines(String command, String[][] expectedText) {
    Log.getLogWriter().info("checkOutputLines-Start");
    String outputText = execCommand(command, false)[0];

    StringBuilder error;
    String[] outputLines = outputText.split("\n"); // split on white space
    int expectedNbrOfLines = (expectedText[0].length > expectedText[1].length) ? expectedText[0].length : expectedText[1].length;
    //Log.getLogWriter().info("checkOutputLines-expectedNbrOfLines=" + expectedNbrOfLines);
    if (outputLines.length < expectedNbrOfLines) {
      error = new StringBuilder("Command output format is incorrect for command '");
      error.append(command);
      error.append("'.  Was expecting at least ");
      error.append(expectedText.length);
      error.append(" lines of output, but received:'");
      error.append(outputLines.length);
      error.append("'.");
      saveError(cmdErrorKey, error);
    }

    String[] expectedStartsWith = expectedText[0];
    String[] expectedEndsWith = expectedText[1];
    String outputLine;
    for (int i = 0;i < outputLines.length;i++) {
      if (i == expectedStartsWith.length && i == expectedEndsWith.length) {
        break;
      }
      outputLine = outputLines[i].trim();
      if (i < expectedStartsWith.length &&
          (null != expectedStartsWith[i] && !outputLine.startsWith(expectedStartsWith[i]))) {
        error = new StringBuilder("Line ").append(i + 1);
        error.append(" of the output is incorrect for command: |").append(command);
        error.append("|\nWas expecting it to start with: |").append(expectedStartsWith[i]);
        error.append("|\nbut received: |").append(outputLine).append("|");
        saveError(cmdErrorKey, error);
      }

      if (i < expectedEndsWith.length &&
          (null != expectedEndsWith[i] && !outputLine.endsWith(expectedEndsWith[i]))) {
        error = new StringBuilder("Line ").append(i + 1);
        error.append(" of the output is incorrect for command: |").append(command);
        error.append("|\nWas expecting it to end with: |").append(expectedEndsWith[i]);
        error.append("|\nbut received: |").append(outputLine).append("|");
        saveError(cmdErrorKey, error);
      }
    }

    Log.getLogWriter().info("checkOutputLines-End");
    return outputText;
  }

  private StringBuilder formatExceptionText(String command, String receivedText) {
    return formatExceptionText(command, receivedText, null);
  }
  private StringBuilder formatExceptionText(String command, String receivedText, String expectedText) {
    return formatExceptionTextDifference(command, receivedText, expectedText);
  }
  private static StringBuilder formatExceptionTextDifference(String command, String receivedText, String expectedText) {
    StringBuilder bldr = new StringBuilder("\nCommand:|");
    bldr.append(command);
    bldr.append("|\nReturned unexpected output: |");
    bldr.append(receivedText);
    if (expectedText != null) {
      bldr.append("|\nExpected output: |");
      bldr.append(expectedText);
    }

    int z = 0;
    for (z = 0;z < expectedText.length() && z < receivedText.length();z++) {
      if (receivedText.charAt(z) != expectedText.charAt(z)) {
        break;
      }
    }
    bldr.append("|\nMatching text was: |");
    bldr.append(receivedText.substring(0, z));
    bldr.append("|");
    return bldr;
  }

  /**
   * Check the command with a variety of bad options and values
   * @param command - the command to test
   */
  private void checkNamedParams(String command) {
    checkNamedParams(command, new CmdOptionInfo[] {});
  }
   /**
   * Check the command with a variety of bad options and values
   * @param command - the command to test
   * @param options
   */
  private void checkNamedParams(String command, CmdOptionInfo[] options) {
    Log.getLogWriter().info("checkNamedParams-Start");

    String badValue = "badValue";
    String badOption = "badOption";
    StringBuilder cmdBuilder;
    StringBuilder errBuilder = null;
    String expectedOutput;

    boolean haveRequiredOptions = false;
    for (CmdOptionInfo option : options) {
      if (option.isRequired()) {
        haveRequiredOptions = true;
        break;
      }
    }
    // Required option(s) section
    StringBuilder reqOpts = null;
    if (haveRequiredOptions) {
      // build the error
      errBuilder = new StringBuilder();
      for (CmdOptionInfo option : options) {
        if (option.isRequired()) {
          if (errBuilder.length() > 0) {
            errBuilder.append("\n");
          }
          errBuilder.append("Parameter \"").append(option.getName())
                    .append("\" is required. Use \"help <command name>\" for assistance.");
        }
      }
      // Call command without required options
      checkCommand(command, errBuilder.toString());

      // Call command without required option(s) and with a bad value(s)
      // build the command
      cmdBuilder = new StringBuilder(command).append(PARM_PREFIX).append(badOption); // Note the bad option
      // build the error
      errBuilder.append("\nParameter ").append(badOption).append(" is not applicable for ").append(command);
      expectedOutput = errBuilder.toString();
      // make the calls
      checkCommand(cmdBuilder.toString(), expectedOutput);
      cmdBuilder.append("="); // Add just the equal sign
      checkCommand(cmdBuilder.toString(), expectedOutput);
      cmdBuilder.append(badValue); // Add a bad value
      checkCommand(cmdBuilder.toString(), expectedOutput);

      // Call the command with the correct required option but with a bad value
      for (CmdOptionInfo option : options) {
        if (option.isRequired()) {
          String errorMsg = option.getErrorMsg();
          if (null != errorMsg && errorMsg.length() > 0) {
            cmdBuilder = new StringBuilder(command).append(PARM_PREFIX).append(option.getName()).append("=").append(badValue);
            expectedOutput = option.getErrorMsg().replace("%1", badValue);
            checkCommand(cmdBuilder.toString(), expectedOutput);
          }
        }
      }

      // build the required information for a valid call to use in the next tests
      reqOpts = new StringBuilder();
      for (CmdOptionInfo option : options) {
        if (option.isRequired()) {
          reqOpts.append(PARM_PREFIX).append(option.getName()).append("=").append(option.getValidValue());
        }
      }
    }

    // Non-Required option(s) section
    for (CmdOptionInfo option : options) {
      if (!option.isRequired()) {
        // set the expected output
        expectedOutput = "Value is required for parameter \"" + option.getName() + "\"";
        // build up the command
        cmdBuilder = new StringBuilder(command);
        if (reqOpts != null) { // if there are any required options, make sure we add them to the command
          cmdBuilder.append(reqOpts);
        }
        cmdBuilder.append(PARM_PREFIX).append(option.getName());
        // Call command with the option and no value
        if (!option.isBoolean()) { // ignore this call if this option is of boolean type
          checkCommand(cmdBuilder.toString(), expectedOutput);
        }
        cmdBuilder.append("=");
        checkCommand(cmdBuilder.toString(), expectedOutput);

        // Call command with the option and a bad value
        if (option.isBoolean()) {
          if (command.equals(CMD_ALTER_RUNTIME) && option.getName().equals("enable-statistics")) {
            expectedOutput = "Value for enable-statistics must be either \"true\" or \"false\"";
          } else {
            expectedOutput = "Value \"" + badValue + "\" is not applicable for \"" + option.getName() + "\".";
          }
        } else {
          expectedOutput = option.getErrorMsg().replace("%1", badValue);
        }
        cmdBuilder.append(badValue);
        if ((command.equals(CMD_DESTROY_INDEX) && (option.getName().equals(PARM_NAME) || option.getName().equals(PARM_REGION)))
            || (command.equals(CMD_REMOVE) && option.getName().equals(PARM_KEY))) {
          checkOutputLines(cmdBuilder.toString(), new String[][] { { expectedOutput }, { } });
        } else {
          checkCommand(cmdBuilder.toString(), expectedOutput);
        }
      }
    }

    // Invalid option section
    // Call command with a bad option and no value
    errBuilder = new StringBuilder("Parameter ").append(badOption).append(" is not applicable for ").append(command);
    expectedOutput = errBuilder.toString();
    cmdBuilder = new StringBuilder(command);
    //if there are any required options, make sure we add them to the command
    if (reqOpts != null) {
      cmdBuilder.append(reqOpts);
    }
    cmdBuilder.append(PARM_PREFIX).append(badOption); // Add a bad option
    checkCommand(cmdBuilder.toString(), expectedOutput);
    cmdBuilder.append("="); // Add just the equal sign
    checkCommand(cmdBuilder.toString(), expectedOutput);
    cmdBuilder.append(badValue); // Add a bad value
    checkCommand(cmdBuilder.toString(), expectedOutput);

    Log.getLogWriter().info("checkNamedParams-End");
  }

  /**
   * Check the command with options specified on separate lines
   * @param command - The command to test
   * @param options - an Array of options to pass to the command
   */
  private void checkMultiLineExecution(String command, CmdOptionInfo[] options) {
    Log.getLogWriter().info("checkMultiLineExecution-Start");

    HostHelper.OSType osType = HostHelper.getLocalHostOS();
    if (HostHelper.isWindows()) {
      saveError(cmdErrorKey, new StringBuilder("Bug #46388 Detected - command continuation character '\\' not working on windows."));
    } else {
      // Multiple-Line section
      CommandStringBuilder singleLine = new CommandStringBuilder(command);
      CommandStringBuilder multiLine  = new CommandStringBuilder(command);

      //if there are any options with valid values, make sure we add them to the command
      for (CmdOptionInfo option : options) {
        String validValue = option.getValidValue();
        if (null != validValue && validValue.length() > 0) {
          singleLine.addOption(option.getName(), validValue);
          multiLine.addNewLine().addOption(option.getName(), validValue);
        }
      }
      // Execute the multi-line command and check the result
      boolean hasError = runCommandCheckForError(multiLine.toString());
      if (hasError) {
        // The multi-line command failed, now try it on one line
        hasError = runCommandCheckForError(singleLine.toString());
        // if it failed on multi-lines but passed on a single-line, then we have an error (bug), otherwise we have a problems with the test
        if (!hasError) {
          saveError(cmdErrorKey, new StringBuilder("An error occurred while attempting to execute command '" + command + "' on multiple lines."));
        } else {
          throw new TestException("There is a problems in the test when testing command '" + command + "' on multiple lines.");
        }
        Log.getLogWriter().info("checkMultiLineExecution-Executing without the ML:hasError=" + hasError);
      }
    }

    Log.getLogWriter().info("checkMultiLineExecution-End");
  }

  /**
   * This tests if the commands that have a region option will work with the leading slash ('/') and without it
   * @param command - the command to test
   * @param regionOption - the region option
   * @param withoutSlash - flag to indicate whether this execution is to use the '/' or not
   */
  private void checkRegionOption(String command, CmdOptionInfo regionOption, boolean withoutSlash) {
    checkRegionOption(command, new CmdOptionInfo[] { }, regionOption, withoutSlash);
  }
  /**
   * This tests if the commands that have a region option will work with the leading slash ('/') and without it
   * @param command - the command to test
   * @param reqOptions - the required options for a successfull execution
   * @param regionOption - the region option
   * @param withoutSlash - flag to indicate whether this execution is to use the '/' or not
   */
  private void checkRegionOption(String command, CmdOptionInfo[] reqOptions, CmdOptionInfo regionOption, boolean withoutSlash) {
    Log.getLogWriter().info("checkRegionOption-Start");

    // build the required information for a valid call to use in the next test
    StringBuilder reqOpts = null;
    if (reqOptions.length > 0) {
      reqOpts = new StringBuilder();
      for (CmdOptionInfo reqOption : reqOptions) {
        reqOpts.append(PARM_PREFIX).append(reqOption.getName()).append("=").append(reqOption.getValidValue());
      }
    }

    StringBuilder cmdBuilder = new StringBuilder(command);
    // add the required options to the command
    if (reqOpts != null) {
      cmdBuilder.append(reqOpts);
    }
    cmdBuilder.append(PARM_PREFIX).append(regionOption.getName()).append("=");
    String regionValue = regionOption.getValidValue();
    Log.getLogWriter().info("checkRegionOption-regionValue(before)" + regionValue);

    if (withoutSlash && regionValue.startsWith("/")) {
      // remove the slash if this call is supposed to test without it
      regionValue = regionValue.substring(1);
    } else if (!withoutSlash && !regionValue.startsWith("/")) {
      // add the slash if this call is supposed to test with it
      regionValue = "/" + regionValue;
    }

    Log.getLogWriter().info("checkRegionOption-regionValue(after)" + regionValue);
    execCommand(cmdBuilder.toString() + regionValue);

    Log.getLogWriter().info("checkRegionOption-End");
  }


  /**
   * Issue command without semico for continuation testing
   */
  private void execCommandLine(String command) {
    try {
      shell.addChars(command + "\n");
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * Execute the given command and log the results. This is called on a command that is
   * expected to succeed as error checking is done on the result.
   *
   * @param command The command to execute
   *
   * @return The presentation String (ie the String output that is presented to the user)
   */
  private String execCommand(String command) {
    String[] resultArr = execCommand(command, true);
    return resultArr[0]; // return the output text
    //return resultArr[1]; // return the presentation string
  }

  /** Execute the given command.
   *
   * @param command The command to execute.
   * @param haltOnCommandFailure If true, then throw an exception if the command does not succeed; if false
   *                             then just return the output (for example, if the test explicitly gives a command
   *                             with bad syntax and is expected to fail, the output is returned for the test to
   *                             determine if the failure is correct).
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  public String[] execCommand(String command, boolean haltOnCommandFailure) {
    try {
      // run the command
      long duration = runCommand(command);
      
      // log the results of the command
      Log.getLogWriter()
      	.info("execCommand-shell.getCompletorOutput() for " + command + "=" + shell.getCompletorOutput());
      Map<String, Object> commandOutput = shell.getCommandOutput();
      Log.getLogWriter().info("execCommand-commandOutput for " + command + "=" + commandOutput);
      boolean exportConfigFilesAreSaved = false;
      for (Object resultObject : commandOutput.values()) {
        if (resultObject instanceof CommandResult) {
          CommandResult cr = (CommandResult) resultObject;
          if (cr.hasIncomingFiles() && cr.getNumTimesSaved() == 0) {
            Log.getLogWriter().info("execCommand-cr.getNumTimesSaved()a" + cr.getNumTimesSaved());
            try {
              Log.getLogWriter().info("Attempting to save file for command " + command);
              cr.saveIncomingFiles(".");
            } catch (IOException e) {
              Log.getLogWriter().info("Could not save file for command " + command);
            }
            Log.getLogWriter().info("execCommand-cr.getNumTimesSaved()b" + cr.getNumTimesSaved());
          } else if (cr.hasIncomingFiles() && cr.getNumTimesSaved() > 0 && command.startsWith(CMD_EXPORT_CONFIG)) {
            exportConfigFilesAreSaved = true;
          }
        }
      }

      String outputText = shell.getOutputText().trim();

      //Log.getLogWriter().info("execCommand-output byte array:\n" + toHexString(outputText.getBytes()));
      
      outputText = outputText.replaceAll("\r","");
      
      int x = outputText.indexOf("\n");
      if(x!=-1) {
        outputText = outputText.substring(x).trim();   // snip off the command echo
      }
      
      if(outputText.startsWith(">")) {     // continuation line is also returned and starts with >
        x = outputText.indexOf("\n");
        if(x!=-1) {
          outputText = outputText.substring(x).trim();   // snip off the continuation
        }        
      }
      
      if(outputText.endsWith(">")) outputText=outputText.substring(0,outputText.lastIndexOf("\n")).trim();
      if(outputText.endsWith(">")) outputText=outputText.substring(0,outputText.lastIndexOf("\n")).trim(); // 2 sometimes!
      
      Log.getLogWriter().info("execCommand-outputText for " + command + ":\n" + outputText);

      // get and log the presentation string
      String presentationStr = "";
      String whiteSpaceErrors = "";
      if (commandOutput.size() == 0) {
        presentationStr = stripSpecialCoding(command, outputText);
      } else if (exportConfigFilesAreSaved) {
        presentationStr = "Files are saved.";
      } else {
        String[] tmpArr = getPresentationString(commandOutput);
        presentationStr = tmpArr[0];
        whiteSpaceErrors = tmpArr[1];
      }

//      presentationStr = outputText;  // saj hook
      
//      Log.getLogWriter().info("execCommand-presentationStr for " + command + ":\n" + presentationStr);
      logCommandOutput(command, presentationStr, whiteSpaceErrors, duration);

      // error checking on results
      checkForFatalErrors(outputText, presentationStr, whiteSpaceErrors);
      if (haltOnCommandFailure) {
        checkForCommandFailure(outputText, commandOutput);
      }

      Log.getLogWriter().info("execCommand-outputText for " + command + ":\n" + outputText);
      Log.getLogWriter().info("execCommand-presentationStr for " + command + ":\n" + presentationStr);
      return new String[] { outputText, presentationStr };
    } finally {
      // clear for next command
      Log.getLogWriter().info("Calling clearEvents() on " + shell);
      shell.clearEvents();
    }
  }

  private String execHelpCommad(String command) {
    try {
      // run the command
      runCommand(command);

      String presentationStr = getHelpPresentationString(shell.getCommandOutput());
      return presentationStr;
    } finally {
      // clear for next command
      Log.getLogWriter().info("Calling clearEvents() on " + shell);
      shell.clearEvents();
    }
  }

  private void execSleepCommad() {
    execSleepCommad("3");
  }
  private void execSleepCommad(String seconds) {
    try {
      // run the command
      runCommand(CMD_SLEEP + " --time=" + seconds);

    } finally {
      // clear for next command
      Log.getLogWriter().info("Calling clearEvents() on " + shell);
      shell.clearEvents();
    }
  }

  /**
   * This method executes the command and returns the command output separated into lines
   * @param command - the command to execute
   * @return - the command output separated into lines
   */
  private ArrayList<String> runDataCommand(String command) {
    Log.getLogWriter().info("runDataCommand-Start-command=" + command);
    ArrayList<String> rtnLines = new ArrayList<String>();

    try {
      // run the command
      runCommand(command);

      // Get the command output
      Map<String, Object> commandOutput = shell.getCommandOutput();
      // Break down the command output into separate lines
      for (Object key : commandOutput.keySet()) {
        Object value = commandOutput.get(key);
        if (value instanceof CommandResult) {
          CommandResult cr = (CommandResult) value;
          cr.resetToFirstLine();
          while (cr.hasNextLine()) {
            String[] lines = cr.nextLine().split("\n");
            for (String line : lines) {
              rtnLines.add(line);
            }
          }
        }
      }
    } finally {
      //clear for next command
      Log.getLogWriter().info("Calling clearEvents() on " + shell);
      shell.clearEvents();
    }
    Log.getLogWriter().info("runDataCommand-End-(" + command + ")=" + rtnLines);
    return rtnLines;
  }

  /**
   * This method simply runs the command and reports back if an error has occurred.
   * @param command - the command to execute
   * @return - boolean whether an error has occurred
   */
  private boolean runCommandCheckForError(String command) {
    Log.getLogWriter().info("runCommandCheckForError-Start-command=" + command);
    boolean hasError = false;

    try {
      // run the command
      runCommand(command);
      hasError = shell.hasError();

    } finally {
      //clear for next command
      Log.getLogWriter().info("Calling clearEvents() on " + shell);
      shell.clearEvents();
    }
    Log.getLogWriter().info("runCommandCheckForError-End-(" + command + ") hasError=" + hasError);
    return hasError;
  }

  private long runCommand(String command) {
    try {
      Log.getLogWriter().info("runCommand-Executing Command " + command +
                              " with command Mgr " + CommandManager.getInstance());
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    long duration = -1;
    try {
      long startTime = System.currentTimeMillis();
      shell.addChars(command).newline();
      shell.waitForOutput();
      duration = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("Command " + command + " took " + duration + " ms");
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    return duration;
  }

  /** Get the output from the shell and return a String equivalent to what *should* be
   *  displayed to the user in gfsh.
   *
   * @return [0] The String that should be a representation of the text presented to the gfsh
   *         user. The word "should" means that we build the presentable string here and we
   *         hope it is the same string built by gfsh.
   *         [1] An error string is any erros were detected.
   */
  private String[] getPresentationString(Map<String, Object> commandOutput) {
    Log.getLogWriter().info("getPresentationString-Start");
    StringBuilder strBldr = new StringBuilder();
    String presentationStr = new String();
    StringBuilder whiteSpaceErrors = new StringBuilder();
    for (Object key : commandOutput.keySet()) {
      Object value = commandOutput.get(key);
      if (value instanceof CommandResult) {
        CommandResult cr = (CommandResult) value;
        cr.resetToFirstLine();
        while (cr.hasNextLine()) {
          String line = cr.nextLine();
          line = line.replaceAll("\r","");  // saj hook2
          strBldr.append(line).append("\n");
          whiteSpaceErrors.append(checkForRightPadding(line));
        }
      }
    }

    presentationStr = strBldr.toString();
    return new String[] { presentationStr, whiteSpaceErrors.toString() };
  }
  
  private String getHelpPresentationString(Map<String, Object> commandOutput) {
    Log.getLogWriter().info("getHelpPresentationString-Start");
    StringBuilder strBldr = new StringBuilder();

    for (Object key : commandOutput.keySet()) {
      Object value = commandOutput.get(key);
      if (value instanceof CommandResult) {
        CommandResult cr = (CommandResult) value;
        cr.resetToFirstLine();
        while (cr.hasNextLine()) {
          String line = cr.nextLine();
          //Log.getLogWriter().info("getHelpPresentationString-line=" + line);
          strBldr.append(splitAndTrim(line));
        }
      }
    }

    return strBldr.toString().trim();
  }

  private String splitAndTrim(String text) {
    Log.getLogWriter().info("splitAndTrim-Start");
    //Log.getLogWriter().info("splitAndTrim-Start-text=" + text);
    StringBuilder strBldr = new StringBuilder();

    String[] lines = text.split("\n");
    for (String line : lines) {
      strBldr.append(line.trim());
    }

    return strBldr.toString();
  }

  private String stripSpecialCoding(String command, String text) {
    Log.getLogWriter().info("stripSpecialCoding-Start");
    //Log.getLogWriter().info("stripSpecialCoding command:" + command + "\ntext:\n|" + text + "|");
    StringBuilder strBldr = new StringBuilder();

    String[] outputLines = text.split("\n");
    //Log.getLogWriter().info("stripSpecialCoding outputLines.length:" + outputLines.length);
    for (String outputLine : outputLines) {
      if (!outputLine.startsWith("Cluster") &&
          !outputLine.startsWith("gfsh") &&
          !outputLine.startsWith("CommandResult") &&
          !outputLine.startsWith(command)) {
        strBldr.append(outputLine.trim()).append("\n");
      }
    }

    String rtnString = strBldr.toString().trim();
    //Log.getLogWriter().info("stripSpecialCoding rtnString:\n|" + rtnString + "|");
    return rtnString;
  }

  /**
   * Look for errors in the output of a gfsh command that are fatal in all occurrences. This means that this method will
   * throw an exception for very bad output in general but will not throw an exeption for a command that failed because,
   * for example, it was given a bad argument intentionally.
   *
   * @param outputText The TestableShell's getOutputText()
   * @param presentationStr The output presented to the gfsh user.
   * @param whiteSpaceErrors A string detailing any white space problems in the presentation string.
   * @throws TestException if a fatal error is detected.
   */
  private void checkForFatalErrors(String outputText, String presentationStr, String whiteSpaceErrors) {
    // check for general errors
    if (null == outputText) {
      throw new TestException("Unexpected output text:" + outputText);
    }
    if ((null == presentationStr) ||
        (presentationStr.contains("ERROR")) ||
        (presentationStr.contains("Exception")) ||
        (presentationStr.contains("null")) ||
        (presentationStr.trim().length() == 0)) {
      throw new TestException("Unexpected command output:" + presentationStr);
    }
    if (whiteSpaceErrors.length() > 0) {
      Log.getLogWriter().info(whiteSpaceErrors); // todo lynn; throw this instead when whitespace problem is fixed
    }
  }

  private void checkForCommandFailure(Object commandOutput) {
    checkForCommandFailure(shell.getOutputText(), commandOutput);
  }
    /** Look for errors in the output of a gfsh command that indicates the command
    *  itself failed. This is called when a command is expected to succeed, rather
    *  than a command that is expected to fail.
    *
    * @param outputText The TestableShell's getOutputText()
    * @param commandOutput The TestableShell's shell.getCommandOutput()()
    *
    * @throws TestException Thrown if the command fails.
    */
  private void checkForCommandFailure(String outputText, Object commandOutput) {
    if (shell.hasError()) {
      throw new TestException("Failed with error " + shell.getError());
    }
    // check for general errors
    if ((outputText.contains("ERROR")) ||
        (outputText.contains("Exception"))) {
      throw new TestException("Unexpected output text:" + outputText);
    }
    String commandOutputStr = commandOutput.toString();
    if ((null == commandOutputStr) ||
        (commandOutput == null) ||
        (commandOutputStr.contains("ERROR")) ||
        (commandOutputStr.contains("Exception"))) {
      throw new TestException("Unexpected output text:" + commandOutputStr);
    }
  }

  /** Check a line of output for extra white space on the right
   *
   * @param aStr The line of output (which could contain multiple lines.
   * @return An error String if any extra white space was found
   */
  private String checkForRightPadding(String aStr) {
    Log.getLogWriter().info("checkForRightPadding-Start");
//    Log.getLogWriter().info("Checking for white space for line:\n"" + aStr);
    String[] tokens = aStr.split("\n");
    StringBuilder errStr = new StringBuilder();
    for (String line: tokens) {
      //for (String )
      int whiteSpaceCount = 0;
      for (int i = line.length()-1; i >= 0; i--) {
        char ch = line.charAt(i);
        if (Character.isWhitespace(ch)) {
          whiteSpaceCount++;
          if (i == 0) { // found a blank line
            errStr.append("\"").append(line).append("\" contains ").append(whiteSpaceCount).append(" white space characters on the right\n");
          }
        } else { // found a non-white space character
          if (whiteSpaceCount > 0) { // we previously found a whitespace character
            errStr.append("\"").append(line).append("\" contains ").append(whiteSpaceCount).append(" white space characters on the right\n");
          }
          break;
        }
      }
    }
    return errStr.toString();
  }

  private void checkFileCreation(String command, String filename) {
    checkFileCreation(command, ".", filename);
  }
  private void checkFileCreation(String command, String path, String filename) {
    Log.getLogWriter().info("checkFileCreation-Start");

    // the show dead-locks command adds a timestamp on the filename, use the following code to find the correct file
    if (command.startsWith(CMD_SHOW_DEADLOCKS)) {
      File theDir = new File(path);
      File[] files = theDir.listFiles();
      for (File aFile : files) {
        Log.getLogWriter().info("checkFileCreation-aFileName:" + aFile.getName());
        if (aFile.getName().startsWith(filename.substring(0, filename.length() - 4))) {
          filename = aFile.getName();
          break;
        }
      }
    }
    // check to make sure the file exists
    File theFile = new File(filename);
    if (!theFile.exists()) {
      throw new TestException("Command '" + command + "' Did not create the file '" + filename + "'.");
    } else if (theFile.length() == 0) {
      throw new TestException("Command '" + command + "' created the file '" + filename + "', but of zero length.");
    }

    // Make sure we changed the file name so we don't find it again
    if (theFile.exists() && (command.startsWith(CMD_SHOW_DEADLOCKS) || command.startsWith(CMD_SHOW_DEADLOCKS))) {
      theFile.renameTo(new File("processed" + filename));
      //theFile.delete();
    }

    Log.getLogWriter().info("checkFileCreation-End");
  }

  /** Write the given command and output to a separate file
   */
  private void logCommandOutput(String command, String output, String errStr, long duration) {
    StringBuilder logStr = new StringBuilder();
    logStr.append("================================================================================\n");
    logStr.append("").append((new Date())).append(" vm_").append(RemoteTestModule.getMyVmid()).append(
      ", thr_").append(RemoteTestModule.getCurrentThread().getThreadId()).append(
      ", pid ").append(RemoteTestModule.getMyPid());
    logStr.append("\nCommand: \"").append(command).append("\"\n");
    logStr.append("Command completed in " + duration + " ms\n");
    logStr.append("Output (starts at beginnning of next line), output length is ").append(output.length()).append(":\n").append(output).append("<--end of output\n");
    if (errStr.length() > 0) {
      logStr.append(errStr).append("\n");
    }
    logStr.append("\n");
    synchronized (this.getClass()) {
      commandOutputFile.print(logStr.toString());
      commandOutputFile.flush();
    }
  }

  /** Connect the testable shell to a locator
   *
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  private String[] connectToLocator() {
    Map<Integer, String> locatorVmIdMap = (Map<Integer, String>) CommandBB.getBB().getSharedMap().get(locatorVmIdsKey);
    if (locatorVmIdMap == null) {
      throw new TestException("You must create your own locator with CommandTest.HydraTask_createLocator to connect gfsh to a locator");
    }
    List<Integer> vmIds = new ArrayList<Integer>();
    List<String> hosts = new ArrayList<String>();
    for (Integer key: locatorVmIdMap.keySet()) {
      vmIds.add(key);
      hosts.add(locatorVmIdMap.get(key));
    }

    // randomly choose a locator if more than one
    int index = TestConfig.tab().getRandGen().nextInt(0, vmIds.size() - 1);
    Integer vmId = vmIds.get(index);
    String host = hosts.get(index);

    // get the correct command parameter and port
    Object value = DistributedSystemBlackboard.getInstance().getSharedMap().get(vmId);
    if ((value == null) || (!(value instanceof DistributedSystemHelper.Endpoint))) {
      throw new TestException("Could not find endpoint for vmId " + vmId + " in " + DistributedSystemBlackboard.class.getName() +
                              ", value in bb is " + ((value == null) ? "null" : (value.getClass().getName())));
    }
    int port = ((DistributedSystemHelper.Endpoint)value).getPort();
    String[] result = execCommand("connect --locator=" + host + "[" + port + "]", true);
    boolean connected = shell.isConnectedAndReady();
    if (!connected) {
      throw new TestException("Connect command failed to connect to locator with vmId " + vmId + " on host " + host + " using port " + port);
    }
    Log.getLogWriter().info("Successfully connected to locator ");
    return result;
  }

  /** Connect the testable shell to a jxmManager member
   * 
   * @param useHttp true if the connect is to an http port, false if the connect is to a jmx port
   *
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  private String[] connectToJMXManager(boolean useHttp) {
    Map<Integer, String> vmIdMap = (Map<Integer, String>) CommandBB.getBB().getSharedMap().get(jmxManagerVmIdsKey);
    if (vmIdMap == null) {
      // there are a couple of reasons why there are no jmxManagers in the blackboard
      // 1) No managers were started either by setting CommandPrms-nbrOfManagers to non-zero or starting managers
      //    using the jmxManagerStart property; in this case you might have meant to connect via a locator by
      //    setting CommandPrms-connectWithLocator to true
      // 2) You did start jmxManagers in some way, but HydraTask_recordManagerStatus() was not run
      throw new TestException("No jmxManagers found in blackboard");
    }
    List<Integer> vmIds = new ArrayList<Integer>();
    List<String> hosts = new ArrayList<String>();
    for (Integer key: vmIdMap.keySet()) {
      vmIds.add(key);
      hosts.add(vmIdMap.get(key));
    }

    // randomly choose a jmxManager member if more than one
    int index = TestConfig.tab().getRandGen().nextInt(0, vmIds.size() -1);
    Integer vmId = vmIds.get(index);
    String host = hosts.get(index);

    // get the correct command parameter and port
    Object value = JMXManagerBlackboard.getInstance().getSharedMap().get(vmId);
    if ((value == null) || (!(value instanceof JMXManagerHelper.Endpoint))) {
      throw new TestException("Could not find endpoint for vmId " + vmId + " in " + JMXManagerBlackboard.class.getName() +
                              ", value in bb is " + ((value == null) ? "null" : (value.getClass().getName())));
    }
    int port = -1;
    String command = null;
    if (useHttp) {
      port = ((JMXManagerHelper.Endpoint)value).getHttpPort();
      command = "connect --use-http --url=http://" + host + ":" + port + "/gemfire/v1";
    } else {
      port = ((JMXManagerHelper.Endpoint)value).getPort();
      command = "connect --jmx-manager=" + host + "[" + port + "]";
    }
    String[] result = execCommand(command, true);
    boolean connected = shell.isConnectedAndReady();
    if (!connected) {
      throw new TestException("Connect command, " + command + ", failed to connect to jmxManager with vmId " + vmId + " on host " + host + " using port " + port);
    }
    Log.getLogWriter().info("Successfully connected to jmxManager with command: " + command);
    return result;
  }

  /** Start a manager if required. A manager will be started if this vmId was previously a manager
   *  or if we have designated a certain number of members to be managers and this one was randomly
   *  selected.
   */
  protected synchronized static void startManagerIfRequired() {
    try {
      Cache theCache = CacheHelper.getCache();
      if (theCache == null) {
        Log.getLogWriter().info("startManagerIfRequired: Not starting a manager because there is no cache in this member");
        return;
      }
      ManagementService service = ManagementService.getManagementService(CacheHelper.getCache());
      if (service.isManager()) {
        Log.getLogWriter().info("startManagerIfRequired: This member is already a manager");
        return;
      }
      String limitManagersToMembers = CommandPrms.getLimitManagersToMembers();
      if ((limitManagersToMembers != null) && !limitManagersToMembers.equalsIgnoreCase("all")) {
        List<String> memberNames = Arrays.asList(limitManagersToMembers.split("-"));
        String myName = System.getProperty("clientName");
        boolean withinLimit = false;
        for (String name: memberNames) {
          if (myName.indexOf(name) >= 0) {
            withinLimit = true;
            break; // this client is in the list of memberNames
          }
        }
        if (!withinLimit) {
          Log.getLogWriter().info("startManagerIfRequired: Not starting a manager because this member is not " + limitManagersToMembers);
          return;
        }
      }
      String key = isManagerKeyPrefix + RemoteTestModule.getMyVmid();
      if (!CommandBB.getBB().getSharedMap().containsKey(key)) { // this member was not a manager before; see if it should become one
        // Read the param to determine how many managers to create
        String nbrOfManagers = CommandPrms.getNbrOfManagers();
        Log.getLogWriter().info("startManagerIfRequired: CommandPrms-nbrOfManagers:" + nbrOfManagers);
        if (!nbrOfManagers.equals("all")) {
          try {
            // Try to convert the string to a number
            int nbrOfMgrsToStart = Integer.parseInt(nbrOfManagers);
            if (nbrOfMgrsToStart <= 0) {
              Log.getLogWriter().info("startManagerIfRequired: Not starting a manager because CommandPrms-nbrOfManagers is " + nbrOfManagers);
              return;
            }
            // if we have a number, then we need to make sure we don't create too many managers
            long mgrCnt = CommandBB.getBB().getSharedCounters().incrementAndRead(CommandBB.managerCount);
            if (mgrCnt > nbrOfMgrsToStart) {
              Log.getLogWriter().info("startManagerIfRequired: Not starting a manager to not exceed CommandPrms-nbrOfManagers " + nbrOfManagers);
              return; // no need to start anymore managers
            }
          } catch (NumberFormatException ex) {
            throw new TestException("Invalid 'nbrOfManagers' option.  Must be a valid number or the word 'all'");
          }
        }
      } else {
        // this member was already a manager previously but isn't now 
        // this must be a restart of the member, so let it become a manager again
      }

      String clientName = System.getProperty("clientName");
      ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
      GemFireDescription gfd = cd.getGemFireDescription();
      Boolean jmxManager = gfd.getJMXManager();
      Log.getLogWriter().info("startManagerIfRequired: Starting a manager");
      try {
        service.startManager();
        CommandBB.getBB().getSharedMap().put(isManagerKeyPrefix + RemoteTestModule.getMyVmid(),  new Boolean(true));
        CommandBB.getBB().getSharedMap().put(expectManagerKeyPrefix + RemoteTestModule.getMyVmid(), new Boolean(true));
        boolean isManager = service.isManager();
        if (!isManager) {
          throw new TestException("Called ManagementService.startManager() but isManager() is " + isManager);
        }
        if (jmxManager == null) {
          if (!clientName.contains("locator")) {
            throw new TestException("startManager() did not throw " + ManagementException.class.getName());
          }
        } else if (!((Boolean)jmxManager)) {
          throw new TestException("startManager() did not throw " + ManagementException.class.getName() +
                                  " but jmxManager is " + jmxManager);
        }
      } catch (ManagementException e) {
        if (jmxManager == null) { // jmxManager was not explicitly set
          if (clientName.contains("locator")) { // this is a locator, jmxManager defaults to true
            // so we should not get this exception
            throw e;
          }
        } else if ((Boolean)jmxManager) { // jmxManager explicitly set to true; should not get this exception
          throw e;
        }
        // only accept a ManagementException if the error text is acceptable
        String errStr = e.toString();
        if (!errStr.contains("Could not start the manager because the gemfire property \"jmx-manager\" is false.")) {
          throw e;
        }
        Log.getLogWriter().info("Caught expected " + e);
        boolean isManager = service.isManager();
        if (isManager) {
          throw new TestException("ManagementService.startManager() threw " + e + " but isManager() is " + isManager);
        }
      }
    } finally {
      HydraTask_recordManagerStatus();
    }
  }

  /** Connect gfsh to either a locator or jmxManager according to the hydra parameter
   *  CommandPrms.connectToLocator.
   *
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  public static String[] connectCLI() {
    String howToConnect = CommandPrms.getHowToConnect();
    if (howToConnect == null) { // use the old hydra param (CommandPrms.connectToLocator) to determine how to connect
      if (CommandPrms.getConnectToLocator()) {
        return testInstance.connectToLocator();
      } else {
        return testInstance.connectToJMXManager(false);
      }
    } else { // use the new hydra param (CommandPrms.howToConnect) to determine how to connect
      if (howToConnect.equals("locator")) {
        return testInstance.connectToLocator();
      } else if (howToConnect.equals("jmxManager")) {
        return testInstance.connectToJMXManager(false);
      } else if (howToConnect.equals("http")) {
        return testInstance.connectToJMXManager(true);
      } else {
        throw new TestException("CommandPrms-howToConnect is set to " + howToConnect);
      }
    }
  }
  
  public static String toHexString(byte[] array) {
    char[] symbols="0123456789ABCDEF".toCharArray();
    char[] hexValue = new char[array.length * 2];

    for(int i=0;i<array.length;i++)
    {
    //convert the byte to an int
    int current = array[i] & 0xff;
    //determine the Hex symbol for the last 4 bits
    hexValue[i*2+1] = symbols[current & 0x0f];
    //determine the Hex symbol for the first 4 bits
    hexValue[i*2] = symbols[current >> 4];
    }
    return new String(hexValue);
  }
  
  /** Return a String logging the given Set with each element on a separate line
   * 
   * @param aSet The Set to log to the return string.
   * @return The string containing the elements of aSet.
   */
  private static String setToString(Set aSet) {
    StringBuilder aStr = new StringBuilder();
    for (Object element: aSet) {
      aStr.append("  " + element + "\n");
    }
    return aStr.toString();
  }
  
  private class CmdOptionInfo {
    private String optionName;
    private String errorMsg;
    private String validValue;
    private boolean required;
    private boolean aBoolean;

    private CmdOptionInfo(String optionName,
                          String errorMsg,
                          String validValue,
                          boolean required,
                          boolean aBoolean) {
      this.optionName = optionName;
      this.errorMsg = errorMsg;
      this.validValue = validValue;
      this.aBoolean = aBoolean;
      this.required = required;
    }

    public String getName() {
      return optionName;
    }

    public String getValidValue() {
      return validValue;
    }

    public void setValidValue(String validValue) {
      this.validValue = validValue;
    }

    public String getErrorMsg() {
      return errorMsg;
    }

    public boolean isRequired() {
      return required;
    }

    public boolean isBoolean() {
      return aBoolean;
    }
  }

  
  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
  
  public static void HydraTask_testAsyncEventQueueCommands() {
    if (isMultiHost()) {
      testInstance.cmdCreateAsyncEventQueueTestMultiHost();
    } else {
      testInstance.cmdListAsyncEventQueueTest();
      testInstance.cmdCreateAsyncEventQueueTest();
    }
  }


  public static void HydraTask_testCreateDiskStoreCommands() {
    if (isMultiHost()) {
      testInstance.cmdCreateDiskStoreTestMultiHost();
    } else {
      testInstance.cmdCreateDiskStoreTest();
    }

  }
  
  /*
   * This test will test the basic functionality of command with optional params.
   * 
   */
  private void cmdCreateAsyncEventQueueTest() {
    Log.getLogWriter().info("cmdCreateAsyncEventQueueTest-Start");
    verifyThatThereShouldNotBeAnyAsyncEventQueue();
    // Check the command with a variety of bad options and values
    int asyncEventQueueSize = getAsycEventQueueSize();
    CmdOptionInfo withInvalidIdOption = new CmdOptionInfo(PARM_ID, "", "-23", true, true);
    CmdOptionInfo withInvalidListenerOption = new CmdOptionInfo(PARM_LISTENER, "", "-23", true, true);
    checkNamedParams(CMD_CREATE_ASYNC_EVENT_QUEUE, new CmdOptionInfo[]{ withInvalidIdOption, withInvalidListenerOption });
    String[] members = getMembers();
    checkCommand(CMD_CREATE_ASYNC_EVENT_QUEUE 
        + prepareCommandParams(PARM_ID, "-23")
        + prepareCommandParams(PARM_LISTENER, "-23"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(onlyOneOccuranceValidator(members))
            .add(noOfTimesOccuranceValidator(members.length, "ERROR: java.lang.ClassNotFoundException: -23"))
            .add(shouldNotHaveValidator("Success")));

    deployJarForListener();
    
  
    // create one async event queue
    checkCommand(CMD_CREATE_ASYNC_EVENT_QUEUE 
        + prepareCommandParams(PARM_ID, "Create_AsyncEventQueue_1")
        + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(onlyOneOccuranceValidator(mergeArrays(members, new String[] { "Success" })))
            .add(noOfTimesOccuranceValidator(members.length - 1, "ERROR: com.gemstone.gemfire.cache.DiskAccessException: For DiskStore: DEFAULT: Could not lock \"./DRLK_IFDEFAULT.lk\". Other JVMs might have created diskstore with same name using the same directory.")));

    asyncEventQueueSize = asyncEventQueueSize + 1;
    // list async event queue should return 1
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize))
            .add(onlyOneOccuranceValidator("Create_AsyncEventQueue_1")));

    //  check what happen if try to create same queue with same id.
    String availableDiskStoreName = getAvailableDiskStoreName();
    checkCommand(CMD_CREATE_ASYNC_EVENT_QUEUE 
        + prepareCommandParams(PARM_ID, "AsyncEventQueue_To_Test_Dup")
        + prepareCommandParams(PARM_DISK_STORE, availableDiskStoreName)
        + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(onlyOneOccuranceValidator(members))
            .add(shouldNotHaveValidatorWithIgnoreCase("ERROR", "EXCEPTION")));;
    asyncEventQueueSize = asyncEventQueueSize + 4;
        
    checkCommand(
        CMD_CREATE_ASYNC_EVENT_QUEUE
            + prepareCommandParams(PARM_ID, "AsyncEventQueue_To_Test_Dup")
            + prepareCommandParams(PARM_DISK_STORE, availableDiskStoreName)
            + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(onlyOneOccuranceValidator(members))
            .add(shouldNotHaveValidatorWithIgnoreCase("SUCCESS"))
            .add(noOfTimesOccuranceValidator(members.length, "ERROR: java.lang.IllegalStateException: A GatewaySender with id  AsyncEventQueue_AsyncEventQueue_To_Test_Dup  is already defined in this cache.")));
   
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
        .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize))
        .add(verifyLineForTabularData("AsyncEventQueue_To_Test_Dup", "100", "false", availableDiskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener" ))
        .add(noOfTimesOccuranceValidator(members.length, "AsyncEventQueue_To_Test_Dup")));
    
    // create some more async event queue with different configuration
    String group = getAvailableGroupName();
    List<String> membersByGroup = getMembersByGroupName(group);
    checkCommand(
        CMD_CREATE_ASYNC_EVENT_QUEUE
            + prepareCommandParams(PARM_ID, "Create_AsyncEventQueue_2")
            + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener")
            + prepareCommandParams(PARM_GROUP, group)
            + prepareCommandParams(PARM_BATCH_SIZE, "1024")
            + prepareCommandParams(PARM_PERSISTENT, "true")
            + prepareCommandParams(PARM_DISK_STORE, availableDiskStoreName)
            + prepareCommandParams(PARM_MAXIMUM_QUEUE_MEMORY, "512")
            + prepareCommandParams(PARM_LISTENER_PARAM_AND_VALUE, "param1")
            + prepareCommandParams(PARM_LISTENER_PARAM_AND_VALUE, "param2#value2"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(membersByGroup.size()))
            .add(onlyOneOccuranceValidator(membersByGroup.toArray(new String[membersByGroup.size()])))
            .add(shouldNotHaveValidatorWithIgnoreCase("ERROR", "EXCEPTION")));

    // list async event queue will return some number and validate the configuration
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize + membersByGroup.size()))
            .add(verifyLineForTabularData("Create_AsyncEventQueue_2", "1024", "true", availableDiskStoreName, "512","com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(noOfTimesOccuranceValidator(membersByGroup.size(), "Create_AsyncEventQueue_2")));
    
    cleanupAsyncEventQueue();
    Log.getLogWriter().info("cmdCreateAsyncEventQueueTest-End");
  }

  private void cmdListAsyncEventQueueTest() {
    Log.getLogWriter().info("cmdListAsyncEventQueueTest-Start");
    String ASYNC_EVENT_QUEUE_NAME = "List_AsyncEventQueue_Test_";
    // list async event queue should return nothing
    final String MSG_NO_ASYNC_EVENT_QUEUE_FOUND = "No Async Event Queues Found";
    int asyncEventQueueSize = getAsycEventQueueSize();
    //TODO: Once we will have option to delete async event queue, we won't require this.
    if(asyncEventQueueSize < 2) {
      checkCommand(CMD_LIST_ASYNC_EVENT_QUEUE, MSG_NO_ASYNC_EVENT_QUEUE_FOUND);
      asyncEventQueueSize = 0;
    } else {
      asyncEventQueueSize = asyncEventQueueSize -2;
    }

    deployJarForListener();
    String[] members = getMembers();
    
    // create some async event queues
    String diskStoreName = getAvailableDiskStoreName();
    for (int i = 0; i < 10; i++) {
      CmdOptionInfo withValidIdOption = new CmdOptionInfo(PARM_ID, "", ASYNC_EVENT_QUEUE_NAME + i, true, false);
      CmdOptionInfo withValidListenerOption = new CmdOptionInfo(PARM_LISTENER, "", "com.hydratest.CreateAsyncEventQueueTestListener", true, false);
      CmdOptionInfo withDiskStoreOption = new CmdOptionInfo(PARM_DISK_STORE, "", diskStoreName, false, false);
      checkMultiLineExecution(CMD_CREATE_ASYNC_EVENT_QUEUE, new CmdOptionInfo[] { withValidIdOption, withValidListenerOption, withDiskStoreOption });
      asyncEventQueueSize = asyncEventQueueSize + members.length;
    }
    
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_0", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_1", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_2", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_3", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_4", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_5", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_6", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_7", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_8", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(verifyLineForTabularData("List_AsyncEventQueue_Test_9", "100", "false", diskStoreName, "100", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_0"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_1"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_2"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_3"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_4"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_5"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_6"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_7"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_8"))
            .add(noOfTimesOccuranceValidator(members.length, "List_AsyncEventQueue_Test_9")) 
            );

    String groupName = getAvailableGroupName();
    diskStoreName = getAvailableDiskStoreName();

    // create one more async event queue
    checkMultiLineExecution(
        CMD_CREATE_ASYNC_EVENT_QUEUE,
        new CmdOptionInfo[] {
            new CmdOptionInfo(PARM_ID, "", ASYNC_EVENT_QUEUE_NAME + 11, true, false),
            new CmdOptionInfo(PARM_LISTENER, "", "com.hydratest.CreateAsyncEventQueueTestListener", true, false),
            new CmdOptionInfo(PARM_GROUP, "", groupName, false, false),
            new CmdOptionInfo(PARM_DISK_STORE, "", diskStoreName, false, false),
            new CmdOptionInfo(PARM_BATCH_SIZE, "", "1024", false, false),
            new CmdOptionInfo(PARM_PERSISTENT, "", "false", false, false),
            new CmdOptionInfo(PARM_MAXIMUM_QUEUE_MEMORY, "", "512", false, false) });

    
    // list async event queue will return old + 1
    List<String> membersByGroup = getMembersByGroupName(groupName);
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize +  membersByGroup.size()))
            .add(verifyLineForTabularData(ASYNC_EVENT_QUEUE_NAME + 11, "1024", "false", diskStoreName, "512", "com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(noOfTimesOccuranceValidator(membersByGroup.size(), ASYNC_EVENT_QUEUE_NAME + 11)));
            

    createAndVerifyAsyncEventQueueByAPI();

    cleanupAsyncEventQueue();
    Log.getLogWriter().info("cmdListAsyncEventQueueTest-End");
  }

  private void cmdCreateDiskStoreTest() {
    Log.getLogWriter().info("cmdCreateDiskStoreTest-Start");
    int diskStoreCount = getAvailableDiskStoreCount();
    // verify no disk store by list disk-store
    checkCommand(CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(diskStoreCount)));
    
    checkNamedParams(CMD_CREATE_DISK_STORE, 
        new CmdOptionInfo[] {
          new CmdOptionInfo(PARM_NAME, "", "DiskStore1", true, false),
          new CmdOptionInfo(PARM_DIR, "", ".", true, false)
      });

    checkCommand(CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(diskStoreCount)));
    
    String availableGroupName = getAvailableGroupNameWithOnlyOneMember();
    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_1")
            + prepareCommandParams(PARM_DIR, "createDiskStore1_")
            + prepareCommandParams(PARM_GROUP, availableGroupName),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(1))
            .add(noOfTimesOccuranceValidator(1, "ERROR: java.lang.IllegalArgumentException: \"createDiskStore1_\" was not an existing directory.")));

    final File diskStoreDir1 = createDiskStoreDir("1", availableGroupName);
    
    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_1")
            + prepareCommandParams(PARM_DIR, diskStoreDir1.getAbsolutePath())
            + prepareCommandParams(PARM_GROUP, availableGroupName),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(1))
            .add(noOfTimesOccuranceValidator(1, "Success")));

    diskStoreCount++;
    // verify one disk store on all member by list disk-store
    checkCommand(
        CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
          .add(noOfLinesValidatorForTabularOutut(diskStoreCount))
          .add(verifyLineForTabularData("CREATE_DISK_STORE_1"))
          .add(noOfTimesOccuranceValidator(1, "CREATE_DISK_STORE_1")));
    
    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_1")
            + prepareCommandParams(PARM_DIR, diskStoreDir1.getAbsolutePath())
            + prepareCommandParams(PARM_GROUP, availableGroupName),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(1))
            .add(noOfTimesOccuranceValidator(1, "Success")));

    // verify one disk store on all member by list disk-store
    checkCommand(
        CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
          .add(noOfLinesValidatorForTabularOutut(diskStoreCount))
          .add(verifyLineForTabularData("CREATE_DISK_STORE_1"))
          .add(noOfTimesOccuranceValidator(1, "CREATE_DISK_STORE_1")));
    
    verifyDiskStore("CREATE_DISK_STORE_1", getMembersByGroupName(availableGroupName).get(0), diskStoreDir1.getAbsolutePath());

    //create the disk store on other members
    String newGroupName = availableGroupName;
    while(newGroupName.equals(availableGroupName)) {
      newGroupName = getAvailableGroupNameWithOnlyOneMember();
    }
    
    final File diskStoreDir2 = createDiskStoreDir("1", newGroupName);

    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_1")
            + prepareCommandParams(PARM_DIR, diskStoreDir2.getAbsolutePath())
            + prepareCommandParams(PARM_GROUP, newGroupName),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(1))
            .add(noOfTimesOccuranceValidator(1, "Success")));
    
    diskStoreCount++;
    
    checkCommand(
        CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
          .add(noOfLinesValidatorForTabularOutut(diskStoreCount))
          .add(noOfTimesOccuranceValidator(2, "CREATE_DISK_STORE_1")));

    verifyDiskStore("CREATE_DISK_STORE_1", getMembersByGroupName(newGroupName).get(0), diskStoreDir2.getAbsolutePath());
   
    
    final File diskStoreDir3 = createDiskStoreDir("2", newGroupName);
 
    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_2")
            + prepareCommandParams(PARM_DIR, diskStoreDir3.getAbsolutePath()
            + prepareCommandParams(PARM_GROUP, newGroupName)),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(1))
            .add(noOfTimesOccuranceValidator(1, "Success")));
    
    // verify two disk store on member of specified group and other will have 1 by list disk-store
    verifyDiskStore("CREATE_DISK_STORE_2", getMembersByGroupName(newGroupName).get(0), diskStoreDir3.getAbsolutePath());
 
    final File diskStoreDir4 = createDiskStoreDir("3", newGroupName);
    // create disk-store with specified custom properties
    checkCommand(
        CMD_CREATE_DISK_STORE
          + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_3")
          + prepareCommandParams(PARM_GROUP, newGroupName)
          + prepareCommandParams(PARM_DIR, diskStoreDir4.getAbsolutePath() + "#143323")
          + prepareCommandParams(PARM_ALLOW_FORCE_COMPACTION, "true")
          + prepareCommandParams(PARM_AUTO_COMPACT, "false")
          + prepareCommandParams(PARM_COMPACTION_THRESHOLD, "67")
          + prepareCommandParams(PARM_MAX_OPLOG_SIZE, "355")
          + prepareCommandParams(PARM_QUEUE_SIZE, "5321")
          + prepareCommandParams(PARM_TIME_INTERVAL, "2023")
          + prepareCommandParams(PARM_WRITE_BUFFER_SIZE, "3110"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(1))
            .add(noOfTimesOccuranceValidator(1, "Success")));

    // verify property by describe command
    verifyDiskStore("CREATE_DISK_STORE_3",
        getMembersByGroupName(newGroupName).get(0), "Yes", "No", "67",
        "355", "5321", "2023", "3110", null, diskStoreDir4.getAbsolutePath(), "143323");
    
    // Destroy all the disk store and clean this up.
    Log.getLogWriter().info("cmdCreateDiskStoreTest-End");

  }
  
  private void cmdCreateDiskStoreTestMultiHost() {
    Log.getLogWriter().info("cmdCreateDiskStoreTestMultiHost-Start");
    int diskStoreCount = getAvailableDiskStoreCount();
    
    // verify no disk store by list disk-store
    checkCommand(CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(diskStoreCount)));
    
    String[] members = getMembers();
    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_MULTI_1")
            + prepareCommandParams(PARM_DIR, "createDiskStore1_"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(noOfTimesOccuranceValidator(members.length, "ERROR: java.lang.IllegalArgumentException: \"createDiskStore1_\" was not an existing directory.")));

    checkCommand(
        CMD_CREATE_DISK_STORE
            + prepareCommandParams(PARM_NAME, "CREATE_DISK_STORE_MULTI_1")
            + prepareCommandParams(PARM_DIR, "."),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(noOfTimesOccuranceValidator(members.length, "Success")));

    // verify one disk store on all member by list disk-store
    checkCommand(
        CMD_LIST_DISKSTORES,
        new GenericCommandOutputValidator()
          .add(noOfLinesValidatorForTabularOutut(diskStoreCount + members.length))
          .add(verifyLineForTabularData("CREATE_DISK_STORE_MULTI_1"))
          .add(noOfTimesOccuranceValidator(members.length, "CREATE_DISK_STORE_MULTI_1")));

    for (int i = 0; i < members.length; i++) {
      verifyDiskStore("CREATE_DISK_STORE_MULTI_1", members[i], null);
    }
   
    // Destroy all the disk store and clean this up.
    Log.getLogWriter().info("cmdCreateDiskStoreTestMultiHost-End");
  }

  private void cmdCreateAsyncEventQueueTestMultiHost() {
    Log.getLogWriter().info("cmdCreateAsyncEventQueueTestMultiHost-Start");
    verifyThatThereShouldNotBeAnyAsyncEventQueue();
    // Check the command with a variety of bad options and values
    int asyncEventQueueSize = getAsycEventQueueSize();
    String[] members = getMembers();

    deployJarForListener();
    
  
    // create one async event queue
    checkCommand(CMD_CREATE_ASYNC_EVENT_QUEUE 
        + prepareCommandParams(PARM_ID, "Create_AsyncEventQueue_Multi_1")
        + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(noOfTimesOccuranceValidator(members.length,  "Success" )));

    asyncEventQueueSize = asyncEventQueueSize + members.length;
    // list async event queue should return 1
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize))
            .add(onlyOneOccuranceValidator("Create_AsyncEventQueue_Multi_1")));

    //  check what happen if try to create same queue with same id.
    checkCommand(CMD_CREATE_ASYNC_EVENT_QUEUE 
        + prepareCommandParams(PARM_ID, "Create_AsyncEventQueue_Multi_1")
        + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener"),
        new GenericCommandOutputValidator()
        .add(noOfLinesValidatorForTabularOutut(members.length))
        .add(onlyOneOccuranceValidator(members))
        .add(shouldNotHaveValidatorWithIgnoreCase("SUCCESS"))
        .add(noOfTimesOccuranceValidator(members.length, "ERROR: java.lang.IllegalStateException: A GatewaySender with id  AsyncEventQueue_Create_AsyncEventQueue_Multi_1  is already defined in this cache.")));
        
   
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
        .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize)));
    
    checkCommand(
        CMD_CREATE_ASYNC_EVENT_QUEUE
            + prepareCommandParams(PARM_ID, "Create_AsyncEventQueue_2_Multi")
            + prepareCommandParams(PARM_LISTENER, "com.hydratest.CreateAsyncEventQueueTestListener")
            + prepareCommandParams(PARM_BATCH_SIZE, "1024")
            + prepareCommandParams(PARM_PERSISTENT, "true")
            + prepareCommandParams(PARM_MAXIMUM_QUEUE_MEMORY, "512")
            + prepareCommandParams(PARM_LISTENER_PARAM_AND_VALUE, "param1")
            + prepareCommandParams(PARM_LISTENER_PARAM_AND_VALUE, "param2#value2"),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(members.length))
            .add(onlyOneOccuranceValidator(members))
            .add(shouldNotHaveValidatorWithIgnoreCase("ERROR", "EXCEPTION")));

    // list async event queue will return some number and validate the configuration
    checkCommand(
        CMD_LIST_ASYNC_EVENT_QUEUE,
        new GenericCommandOutputValidator()
            .add(noOfLinesValidatorForTabularOutut(asyncEventQueueSize + members.length))
            .add(verifyLineForTabularData("Create_AsyncEventQueue_2_Multi", "1024", "true", "", "512","com.hydratest.CreateAsyncEventQueueTestListener"))
            .add(noOfTimesOccuranceValidator(members.length, "Create_AsyncEventQueue_2_Multi")));
    
    
    cleanupAsyncEventQueue();
    Log.getLogWriter().info("cmdCreateAsyncEventQueueTestMultiHost-End");
  }

  private File createDiskStoreDir(String diskStoreName, String availableGroupName) {
    final File diskStore1Dir1 = new File(new File(".").getAbsolutePath(), "DiskStore_" + diskStoreName + "."  + availableGroupName);
    diskStore1Dir1.mkdir();
    return diskStore1Dir1;
  }

  private void verifyDiskStore(String diskStoreName, String diskStoreMember, String allowForceCompaction, String autoCompaction, 
      String compactionThreshold, String maxOplogSize, String queueSize, String timeInterval, 
      String writeBufferSize, String pdxMetaDataStored, String diskStoreDirName, String diskStoreSize) {
    checkCommand(
        CMD_DESCRIBE_DISKSTORE
            + prepareCommandParams(PARM_NAME, diskStoreName)
            + prepareCommandParams(PARM_MEMBER, diskStoreMember),
        new GenericCommandOutputValidator()
            .add(noOfLinesValidator(17))
            .add(verifyLineForDescribe("Disk Store ID"))
            .add(verifyLineForDescribe("Disk Store Name",diskStoreName))
            .add(verifyLineForDescribe("Member Name",diskStoreMember))
            .add(verifyLineForDescribe("Allow Force Compaction", allowForceCompaction == null ? "No" : allowForceCompaction))
            .add(verifyLineForDescribe("Auto Compaction", autoCompaction == null ? "Yes" : autoCompaction))
            .add(verifyLineForDescribe("Compaction Threshold", compactionThreshold == null ? "50" : compactionThreshold))
            .add(verifyLineForDescribe("Max Oplog Size", maxOplogSize == null ? "1024" : maxOplogSize))
            .add(verifyLineForDescribe("Queue Size", queueSize == null ? "0" : queueSize))
            .add(verifyLineForDescribe("Time Interval", timeInterval == null ? "1000" : timeInterval))
            .add(verifyLineForDescribe("Write Buffer Size", writeBufferSize == null ? "32768" : writeBufferSize))
            .add(verifyLineForDescribe("PDX Serialization Meta-Data Stored", pdxMetaDataStored == null ? "No" : pdxMetaDataStored))
            .add(verifyLineForTabularData(diskStoreDirName, diskStoreSize)));

  }
  
  private void verifyDiskStore(String diskStoreName, String diskStoreMember, String diskStoreDirName) {
    verifyDiskStore(diskStoreName, diskStoreMember, null, null, null, null,
        null, null, null, null, diskStoreDirName, "2147483647");
  }

  private void createAsyncEventQueueByAPI() {
    // TODO impl pending
    
  }

  private void deployJarForListener() {
    try {
      // Deploy a JAR file with an AsyncEventListener that can be instantiated
      // on each server
      final File jarFile = new File(new File(".").getAbsolutePath(),
          "CreateAsyncEventQueueTest.jar");
      Set<String> filesToBeDeleted = new CopyOnWriteArraySet();
      filesToBeDeleted.add(jarFile.getAbsolutePath());

      ClassBuilder classBuilder = new ClassBuilder();
      byte[] jarBytes = classBuilder
          .createJarFromClassContent(
              "com/hydratest/CreateAsyncEventQueueTestListener",
              "package com.hydratest;"
                  + "import java.util.List; import java.util.Properties;"
                  + "import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2; "
                  + "import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;"
                  + "import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;"
                  + "public class CreateAsyncEventQueueTestListener implements Declarable2, AsyncEventListener {"
                    + "Properties props;"
                    + "public boolean processEvents(List<AsyncEvent> events) { return true; }"
                    + "public void close() {}"
                    + "public void init(final Properties props) {this.props = props;}"
                    + "public Properties getConfig() {return this.props;}" 
                  + "}");
      writeJarBytesToFile(jarFile, jarBytes);

      execCommand("deploy --dir=" + jarFile.getAbsolutePath());
    } catch (IOException e) {
      throw new TestException(
          "There is a problems in the test when trying to deploy jar for async event queue", e);
    }
  }

  
  private void undeployJarForListener() {
    execCommand("undeploy --jar=CreateAsyncEventQueueTest.jar");
  }

  private String prepareCommandParams(String paramName, String paramValue) {
    return new StringBuilder(PARM_SPACE).append(PARM_PREFIX).append(paramName).append(PARM_VALUE_SEP)
        .append(paramValue).append(PARM_SPACE).toString();
  }

  private int getAvailableDiskStoreCount() {
    int lines = getOutputLines(CMD_LIST_DISKSTORES);
    lines = lines - 2;
    return lines < 0 ? 0 : lines;
  }

  private int getAsycEventQueueSize() {
    int lines = getOutputLines(CMD_LIST_ASYNC_EVENT_QUEUE);
    lines = lines - 2;
    return lines < 0 ? 0 : lines;
  }
  
  private int getOutputLines(String command) {
    ArrayList<String> outputText = runDataCommand(command);
    return outputText.size();
  }

  private String getAvailableDiskStoreName() {
    String memberName = parseMemberName(getMemberInfo(getRandomMember()));
    String diskStoreName = findDiskStoreName(memberName, execCommand(CMD_LIST_DISKSTORES));
    return diskStoreName;
  }

  private void createAndVerifyAsyncEventQueueByAPI() {
    // is it possible to create async event queue by API and verify here.
    //TODO: Need to take help from rahul to implement it.

    createAsyncEventQueueByAPI();
   // checkCommand(CMD_LIST_ASYNC_EVENT_QUEUE, new SampleCommandOutputValidator());
  }

  private void cleanupAsyncEventQueue() {
    // TODO : delete all the existing async event queue.
    undeployJarForListener();
   // checkCommand(CMD_LIST_ASYNC_EVENT_QUEUE, new SampleCommandOutputValidator());
  }
  
  private void verifyThatThereShouldNotBeAnyAsyncEventQueue() {
    //  TODO: Enable it when we are able to destroy the existing event from List async event queue test
    //  final String MSG_NO_ASYNC_EVENT_QUEUE_FOUND = "No Async Event Queues Found";
    //  checkCommand(CMD_LIST_ASYNC_EVENT_QUEUE, MSG_NO_ASYNC_EVENT_QUEUE_FOUND);
  }

  
  public String[] mergeArrays(String[] mainArray, String[] addArray) {
    String[] finalArray = new String[mainArray.length + addArray.length];
    System.arraycopy(mainArray, 0, finalArray, 0, mainArray.length);
    System.arraycopy(addArray, 0, finalArray, mainArray.length, addArray.length);
    return finalArray;
  }

  private String[] getMembers() {
    Map<String, String> memberMap = FederationBlackboard.getBB().getMemberONs();
    String[] members = new String[memberMap.size()];
    int i = 0;
    for (String member : memberMap.values()) {
      members[i++] = parseMemberName(member);
    }
    return members;
  }
  
  private class GenericCommandOutputValidator implements CommandOutputValidator {
    Set<ResultValidator> validators = new HashSet<ResultValidator>();
    private Set<String> errors = new HashSet<String>();

    public GenericCommandOutputValidator add(ResultValidator validator) {
      if (validator != null) {
        validators.add(validator);
      }
      return this;
    }

    @Override
    public boolean validate(Map<String, Object> commandOutput) {
      String output = getPresentationString(commandOutput)[0];
      Log.getLogWriter().info("Command output to validate: \n" + output);
      for (ResultValidator validator : validators) {
        Log.getLogWriter().info("Command output validation result : " + validator.validate(output) + "");  
        if (validator.validate(output) == ResultValidator.Status.ERROR) {
          errors.add(validator.getMessage());
        }
      }
      return errors.isEmpty() ? true : false;
    }

    @Override
    public String getFormattedErrors() {
      StringBuilder sb = new StringBuilder();
      for (String error : errors) {
        sb.append(error).append("\n");
      }
      return sb.toString();
    }
  }

  private List<String> getMembersByGroupName(String groupName) {
    try {
      Map<String, Object> commandOutput = executeCommand(CMD_LIST_MEMBERS
          + PARM_SPACE + PARM_PREFIX + PARM_GROUP + PARM_VALUE_SEP + groupName);
      for (Object resultObject : commandOutput.values()) {
        if (resultObject instanceof CommandResult) {
          CommandResult cr = (CommandResult) resultObject;
          if (cr.getType() == ResultData.TYPE_TABULAR) {
            TabularResultData table = (TabularResultData) cr.getResultData();
            List<String> memberNames = table.retrieveAllValues("Name");
            return memberNames;
          }
        }
      }
    } finally {
      shell.clearEvents();
    }
    Log.getLogWriter().info("Not able to find any Members with groupName : " + groupName + ". This may led to failurer in test.");
    return null;
  }

  private static boolean isMultiHost() {
    Vector hosts = TestConfig.getInstance().getPhysicalHosts();
    Set<String> uniqueuHosts = new HashSet<String>(hosts);
    boolean isMultiHost = uniqueuHosts.size() > 1;
    Log.getLogWriter().info("This test is configured for " + (isMultiHost ? " multi host" : "single host"));
    return isMultiHost;
  }

  
  /**
   * Return a random group name defined in any member in this test with only one
   * member It will try to find the group with single member 5 times and if not
   * found then return random group name.
   * 
   * @return The name of a group
   */
  private String getAvailableGroupNameWithOnlyOneMember() {
    int i = 0;
    while (true) {
      Log.getLogWriter().info("Trying again to find group name with single member only. retry count :" + i);
      String groupName = getAvailableGroupName();
      List<String> members = getMembersByGroupName(groupName);
      if (members.size() == 1 || ++i == 5) {
        if (i == 5) {
          Log.getLogWriter()
              .info(
                  "Tried to find group name with single member but did not find anything. returning random group name which can fail test case");
        }
        return groupName;
      }
    }
  }

  /** Wait for 30 seconds of silence to make sure all events have been delivered.
   * 
   */
  public static void waitForSilence() {
    SilenceListener.waitForSilence(30, 1000);
  }
  
  /** Verify bucket copies in all regions in this test
   * 
   */
  public static void verifyBucketCopies() {
    Cache aCache = CacheHelper.getCache();
    if (aCache == null) {
      Log.getLogWriter().info("Not verifying bucket copies in this member, there is no cache");
      return;
    }
    Set<Region> regionSet = new HashSet(aCache.rootRegions());
    Set<Region> rootRegions = new HashSet(regionSet);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    for (Region aRegion: regionSet) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        Log.getLogWriter().info(PrState.getPrPicture(aRegion));
        int rc = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
        Log.getLogWriter().info("Verifying bucket copies for " + aRegion.getFullPath());
        ParRegUtil.verifyBucketCopies(aRegion, rc);
      }
    }
  }
}
