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
package splitBrain;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import newWan.WANTestPrms;
import util.*;
import hydra.*;
import hydra.blackboard.*;
import event.*;
import cq.CQUtil;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.MembershipTestHook;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.management.ManagementException;

/**
 * 
 * Test to execute entry operations while killing coordinators, lead
 * members and the like.
 *
 * @see splitBrain.SplitBrainPrms
 * @see splitBrain.SplitBrainBB
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.5
 */
public class NetworkPartitionTest extends util.OperationsClient {

  // SBListener will set this to true if we see a RegionDestroyedEvent w/
  // Operation.FORCED_DISCONNECT
  static protected boolean forcedDisconnect = false;

  // Single instance in this VM
  static protected NetworkPartitionTest testInstance;
  static Region aRegion;     
  static DistributedSystem oldSystem;
  static String losingSideHost = null;
  static boolean enableNetworkHelper = true;
  static boolean survivingSideGiiClient = false;
  static final String SNAPSHOT_KEY = "regionSnapshot";
  
/* hydra task methods */
/* ======================================================================== */
// Server methods
    public synchronized static void HydraTask_initializeBridgeServer() {
       if (testInstance == null) {
          testInstance = new NetworkPartitionTest();
          testInstance.initializeOperationsClient();
          testInstance.initializePrms();
          testInstance.initializeBridgeServer();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeServer() {

       if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
          SplitBrainBB.addExpectForcedDisconnect(RemoteTestModule.getMyVmid());
       }

       // create cache/region 
       if (CacheHelper.getCache() == null) {
         CacheHelper.createCache(ConfigPrms.getCacheConfig());
         String regionConfig = ConfigPrms.getRegionConfig();
         RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
         AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);

         // install the disk store before creating the region, if needed
         if (requiresPersistence(rd)) {
           String diskStoreConfig = ConfigPrms.getDiskStoreConfig();
           DiskStoreHelper.createDiskStore(diskStoreConfig);
           factory.setDiskStoreName(diskStoreConfig);
         }

         aRegion = RegionHelper.createRegion(rd.getRegionName(), factory);

         CacheServer server =
             BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
         createGatewayHub();
       }
       if (SBUtil.isLeadMember()) {
          Log.getLogWriter().info("This VM is currently the LeadMember");
       }

       // If we're on the losing side of the partition, add this vmId (and memberId) to the lists of vms expecting forced disconnects
       DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
       if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
          SplitBrainBB.addMember(dm, false);       // member of losingPartition
       } else {
          SplitBrainBB.addMember(dm, true);          // member of survivingPartition
       }
    }

    
    /** Return the xml file name for the vm with the given ID.
     *  @pararm vmID The vmID to get the xml file name for. 
     */
    public static String getXmlFileName() {
      int vmID = RemoteTestModule.getMyVmid();
      String clientName = RemoteTestModule.getMyClientName();
      return "vm_" + vmID + "_" + clientName + ".xml";
    }
    

    public synchronized static void HydraTask_initializeBridgeClient() {
       if (testInstance == null) {
          testInstance = new NetworkPartitionTest();
          testInstance.initializeOperationsClient();
          testInstance.initializePrms();
          testInstance.initializeBridgeClient();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeClient() {
       // create cache/region 
       if (CacheHelper.getCache() == null) {
         CacheHelper.createCache(ConfigPrms.getCacheConfig());
         String regionConfig = ConfigPrms.getRegionConfig();
         RegionHelper.createRegion(regionConfig);
         RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);;
         aRegion = RegionHelper.getRegion(rd.getRegionName());
       }

       // initialize CQService, if needed
       if (TestConfig.tab().booleanAt(cq.CQUtilPrms.useCQ, false)) {
          CQUtil.initialize();
          CQUtil.initializeCQService();
          CQUtil.registerCQ(aRegion);
       } else {
          aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("Registered interest in ALL_KEYS");
       }
    }

// P2P methods
    /* 
     * Method to connect to the DS (without creating a cache, etc)
     * to aid in placement of leadMember 
     */
    public synchronized static void HydraTask_connectToDS() {
       DistributedSystemHelper.connect();
    }

    /**
     *  Create the cache and Region.  Check for forced disconnects (for gii tests)
     */
    public synchronized static void HydraTask_initialize() {
      initializeInstance(false);
    }

    /**
     *  Create the cache and Region.  Check for forced disconnects (for gii tests)
     */
    public synchronized static void HydraTask_initializeFromXML() {
      initializeInstance(true);
    }
    
    /**
     * initialize a test instance
     * 
     * @param usingXML whether to generate and use XML to define the cache
     */
    private synchronized static void initializeInstance(boolean usingXML) {
       if (testInstance == null) {
          testInstance = new NetworkPartitionTest();
          testInstance.initializeOperationsClient();
          testInstance.initializePrms();
 
          try {
            if (usingXML) {
              testInstance.initializeFromXML();
            } else {
              testInstance.initialize();
            }
          } catch (ManagementException me) {
             checkForForcedDisconnect(me);
          } catch (CancelException cce) {
             checkForForcedDisconnect(cce);
          } catch (SystemConnectException sce) {
             checkForForcedDisconnect(sce);
          } catch (Exception e) {
             Log.getLogWriter().info("initialize threw Exception " + e + ", forcedDisconnect = " + forcedDisconnect);
             throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
          }
       }
    }
 
    /**
     * If appropriate, adds this client to the set that are expected to
     * receive a forced-disconnect.
     */
    public static void HydraTask_addExpectForcedDisconnectClient() {
      if (losingSideHost == null) {
        setLosingSideHost();
      }
      Log.getLogWriter().info("my host = " + RemoteTestModule.getMyHost() + " and the expected losing side is " + losingSideHost);
      if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
        SplitBrainBB.addExpectForcedDisconnect(RemoteTestModule.getMyVmid());
      }
    }
    
    /**
     * In order for an auto-reconnect to initialize the cache we need to
     * have a cache.xml file for each client that's expected to reconnect
     */
    private void generateCacheXml() {
      String xmlFileName = getXmlFileName();
      File f = new File(xmlFileName);
      if (f.exists()) {
        try {
          f.delete();
        } catch (SecurityException e) {
          throw new TestException("unable to delete XML file", e);
        }
      }
//      DeclarativeGenerator.createDeclarativeXml(getXmlFileName(), CacheHelper.getCache(), false, true);
      CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(),
          null /*dynamicRegionConfig*/,
          ConfigPrms.getRegionConfig(),
          null /*regionNames*/,
          ConfigPrms.getBridgeConfig(),
          ConfigPrms.getPoolConfig(), 
          ConfigPrms.getDiskStoreConfig(),
          null,
          getXmlFileName());
      }


    
    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initialize() {

      if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
         SplitBrainBB.addExpectForcedDisconnect(RemoteTestModule.getMyVmid());
      }

      // create cache/region (and connect to the DS)
      if (CacheHelper.getCache() == null) {
         try {
            CacheHelper.createCache(ConfigPrms.getCacheConfig());
         } catch (ManagementException me) {
            if (me.getCause() instanceof RegionDestroyedException) {
               // re-throw and let the caller handle this
               throw me;
            }
         } catch (SystemConnectException sce) {
            // re-throw and let caller handle this 
            throw sce;
         }

         if (SBUtil.isLeadMember()) {
            Log.getLogWriter().info("This VM is currently the LeadMember");
         }

        String regionConfig = ConfigPrms.getRegionConfig();
        RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
        AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);

        // In giiUnion tests, we must override the giiClients dataPolicy w/replicate
        DataPolicy dataPolicy = SplitBrainPrms.getGiiDataPolicy();   // defaults to null
        if (dataPolicy != null) {
           factory.setDataPolicy(dataPolicy);
        }

        // install the disk store before creating the region, if needed
        if (requiresPersistence(rd)) {
          String diskStoreConfig = ConfigPrms.getDiskStoreConfig();
          DiskStoreHelper.createDiskStore(diskStoreConfig);
          factory.setDiskStoreName(diskStoreConfig);
        }

        String regionName = rd.getRegionName();
        aRegion = RegionHelper.createRegion(regionName, factory);

        createGatewayHub();
        
        performInitialChecks(regionName);
      }

   }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper) using XML.  Tests
     * that allow auto-reconnect must initialize with XML.
     */
    protected void initializeFromXML() {

       // create cache/region (and connect to the DS)
       if (CacheHelper.getCache() == null) {
          try {
            // Auto-reconnect requires that the cache be described in cache.xml.
              String xmlFileName = getXmlFileName();
              generateCacheXml();
              CacheHelper.createCacheFromXml(xmlFileName);
          } catch (ManagementException me) {
             if (me.getCause() instanceof RegionDestroyedException) {
                // re-throw and let the caller handle this
                throw me;
             }
          } catch (SystemConnectException sce) {
             // re-throw and let caller handle this 
             throw sce;
          }

          if (SBUtil.isLeadMember()) {
             Log.getLogWriter().info("This VM is currently the LeadMember");
          }

          String regionConfig = ConfigPrms.getRegionConfig();
          RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
          String regionName = rd.getRegionName();
          aRegion = RegionHelper.getRegion(regionName);
          
          performInitialChecks(regionName);

       } 
    }
    
    private void performInitialChecks(String regionName) {
      // If we're on the losing side of the partition, add this vmId to the list of vms expecting forced disconnects
      DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
      if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
         SplitBrainBB.addMember(dm, false);    // member of losingPartition
      } else {
         SplitBrainBB.addMember(dm, true);     // member of SurvivingPartition
         this.survivingSideGiiClient = TestConfig.tasktab().booleanAt(SplitBrainPrms.isGiiClient, TestConfig.tab().booleanAt(SplitBrainPrms.isGiiClient, false));
         Log.getLogWriter().info("survivingSideGiiClient = " + survivingSideGiiClient);
      }


     // for giiUnion tests, we expect partial set of keys
     // for giiPreference tests, region should be cleared if forcedDisconnect occurs during gii
     if (survivingSideGiiClient) {
        boolean expectEmptyRegion = TestConfig.tasktab().booleanAt(SplitBrainPrms.expectEmptyRegion, TestConfig.tab().booleanAt(SplitBrainPrms.expectEmptyRegion, false));
        // Display the number of entries in the region 
        int regionSize = aRegion.keySet().size();
        Log.getLogWriter().info("Region " + regionName + " has regionSize = " + regionSize);

        long maxKeys = SplitBrainBB.getBB().getSharedCounters().read(SplitBrainBB.loadClientRegionSize);
        if (expectEmptyRegion) {   // expect to get all or nothing, no partial giis
           if ((regionSize != 0) && (regionSize != maxKeys)) {   
              throw new TestException("Expected an empty region or a complete gii (forcedDisconnect during getFromOne), but found " + regionSize + " of " + maxKeys + " entries in region.");
           }
        } else if ((regionSize <= 0) || (regionSize > maxKeys)) {  // giiUnion
           throw new TestException("Expected a partial image, but found " + regionSize + " of " + maxKeys + " entries in region.");
        }
     }
    }

    
    /** call this when the cache has been reconnected after a forced-disconnect */
    protected void reconnected() {
      // fetch the new region reference
      aRegion = CacheHelper.getCache().getRegion(aRegion.getFullPath()); 
    }

    /**
     *  Hydra task to create a blackboard snapshot of the region contents
     */
    public static void HydraTask_createSnapshot() {
      if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
        Log.getLogWriter().info("Not creating region snapshot; this is the losing side");
      } else {
        Log.getLogWriter().info("Creating region snapshot for region " + aRegion.getFullPath());
        Map regionSnapshot = new HashMap();
        for (Object key: aRegion.keySet()) {
          Object value = aRegion.get(key);
          if (value instanceof BaseValueHolder) {
            value = ((BaseValueHolder)value).myValue;
          }
          regionSnapshot.put(key, value);
        }
        Log.getLogWriter().info("Writing region snapshot of size " + regionSnapshot.size() + " to blackboard: " + regionSnapshot);
        SplitBrainBB.getBB().getSharedMap().put(SNAPSHOT_KEY, regionSnapshot);
      } 
    }

    protected boolean requiresPersistence(RegionDescription rd) {
      return rd.getDataPolicy().withPersistence()
             || (rd.getEvictionAttributes() != null &&
                 rd.getEvictionAttributes().getAction().isOverflowToDisk());
    }

   /* 
    * Creates the GatewayHub (if configured)
    */
   protected void createGatewayHub() {
      String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
      // Gateway initialization (if needed)
      if (gatewayHubConfig != null) {
         GatewayHubHelper.createGatewayHub(gatewayHubConfig);
      }
   }

    /* 
     * Initializes SplitBrain test params for this instance
     */
    protected void initializePrms() {
       // Whether or not to enable the NetworkHelper drop/restore commands
       this.enableNetworkHelper = TestConfig.tab().booleanAt(SplitBrainPrms.enableNetworkHelper, true);
       Log.getLogWriter().info("NetworkHelper commands are " + (enableNetworkHelper ? "" : " not ") + "enabled");


       setLosingSideHost();
    }

    private static void setLosingSideHost() {
      // Store the losingPartition hostName for use in processing
      // Shutdown and CacheClosedExceptions after Network Partition
      String losingPartition = TestConfig.tab().stringAt(SplitBrainPrms.losingPartition);
      if ((losingPartition.equalsIgnoreCase("host1")) || (losingPartition.equalsIgnoreCase("host2"))) {
         losingSideHost = TestConfig.getInstance().getHostDescription(losingPartition).getHostName();
         SplitBrainBB.putLosingSideHost( losingSideHost );
         Log.getLogWriter().info("Partition on " + losingPartition + " running on " + losingSideHost + " is not expected to survive the network Partition");
      } else {
         throw new HydraConfigException("losingPartition must specified as host1 or host2, configured as " + losingPartition);
      }
    }

    /**
     * Starts a gateway hub in a VM that previously created one, after creating
     * gateways
     */
    public static void startGatewayHubTask() {
      String gatewayConfig = ConfigPrms.getGatewayConfig();
      testInstance.startGatewayHub(gatewayConfig);
    }

    /**
     * Starts a gateway hub in a VM that previously created one, after creating
     * gateways.
     */
    private void startGatewayHub(String gatewayConfig) {
      GatewayHubHelper.addGateways(gatewayConfig);
      GatewayHubHelper.startGatewayHub();
    }

    /**
     * Creates GatewaySender ids based on the
     * {@link ConfigPrms#gatewaySenderConfig}.
     */
    public synchronized static void HydraTask_createGatewaySenderIds() {
      String senderConfig = ConfigPrms.getGatewaySenderConfig();
      GatewaySenderHelper.createGatewaySenderIds(senderConfig);
    }
    
    /**
     * Creates start new wan components i.e senders based on {@link ConfigPrms#gatewaySenderConfig}
     * and receivers based on {@link ConfigPrms#gatewayReceiverConfig}. 
     */
    public synchronized static void HydraTask_initWanComponents(){
      // create and start receivers
      String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
      GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
      
      //create and start sender
      String senderConfig = ConfigPrms.getGatewaySenderConfig();
      GatewaySenderHelper.createAndStartGatewaySenders(senderConfig);
    }
    
    /**
     *  loadRegion 
     */
    public static void HydraTask_loadRegion() {
        testInstance.loadRegion();
    }

    /**
     *  loads the test region with SplitBrainBB.maxKeys entries.
     *  Note that this task can be accomplished by multiple threads.
     */
    protected void loadRegion() {
       long limit = 60000;
       long startTime = System.currentTimeMillis();
       int maxKeysToCreate = TestConfig.tab().intAt(SplitBrainPrms.maxKeys);

       do {
          if (NameFactory.getPositiveNameCounter() >= maxKeysToCreate) {
             String reason = "In loadRegion, maxKeysToCreate = " + maxKeysToCreate + ", regionSize is " + aRegion.size();
             SplitBrainBB.getBB().getSharedCounters().add(SplitBrainBB.loadClientRegionSize, aRegion.size());
             throw new StopSchedulingTaskOnClientOrder(reason);
          }
          addEntry(aRegion);
       } while (System.currentTimeMillis() - startTime < limit);
    }


    /**
     *  Performs puts/gets on entries in TestRegion
     *  Allows Shutdown and CacheClosedException if the result of forcedDisconnect
     */
    public static void HydraTask_doEntryOperations() {
        try {
           testInstance.doEntryOperations();
        } 
        catch (CancelException cce) {
           checkForForcedDisconnect(cce);
        } catch (TransactionDataNodeHasDepartedException e) {
           // ignoring (as we know we can lose the data host during network partition)
           Log.getLogWriter().info("doEntryOperations threw Exception " + e + ", forcedDisconnect = " + forcedDisconnect);
        } catch (TransactionDataRebalancedException e) {
           // not expecting rebalancing in these tests
           Log.getLogWriter().info("doEntryOperations threw Exception " + e + ", forcedDisconnect = " + forcedDisconnect);
           throw new TestException("doEntryOperations caught Exception " + TestHelper.getStackTrace(e));
        } catch (Exception e) {
           Log.getLogWriter().info("doEntryOperations threw Exception " + e + ", forcedDisconnect = " + forcedDisconnect);
           throw new TestException("doEntryOperations caught Exception " + TestHelper.getStackTrace(e));
        }
    }

    protected void doEntryOperations() {
      if (oldSystem == null) { // bug #49818 - cache the system before doing work
        oldSystem = hydra.DistributedSystemHelper.getDistributedSystem();
      }
       super.doEntryOperations(aRegion);
    }
    
    protected boolean waitForReconnect() {
      if (oldSystem.isReconnecting()) {
        try {
          // TODO this should be configurable
          return oldSystem.waitUntilReconnected(4, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          return false;
        }
      }
      DistributedSystem sys = hydra.DistributedSystemHelper.getDistributedSystem();
      return sys != null && sys != oldSystem;
    }

    /**
     * Close the cache (and disconnect from the DS)
     * This should prevent the ShutdownException: no down protocol available!
     * during Shutdown (if locators (client vms) are shut down prior to 
     * application client vms
     */
    public static void closeCacheAndDisconnectFromDS() {
       CacheHelper.closeCache();
       DistributedSystemHelper.disconnect();
       testInstance = null;
    }

    /**
     * API for SBListener to set the forcedDisconnect flag to indicate that
     * a RegionDestroyedEvent (FORCED_DISCONNECT) has been processed in this VM.
     */
    public static void setForcedDisconnect() {
        forcedDisconnect = true;
    }

    /**
     * check ShutdownExceptions and CacheClosedExceptions for underlying 
     * ForcedDisconnectExceptions (Caused by within the reported exception
     * or if this VM processed a RegionDestroyedException with Operation
     * FORCED_DISCONNECT.  
     *
     * This method verifies that ForcedDisconnects are only reported for
     * the 'losing' partition and throws a TestException if not expected.
     * Adds to forcedDisconnectList in blackboard if reported by client in
     * losing partition.  (Used by ENDTASK to verify correct system response).
     */
    static private void checkForForcedDisconnect(Exception e) {
       Log.getLogWriter().info("checkForForcedDisconnect processed Exception " + e);
       String errStr = e.toString();
       boolean causedByForcedDisconnect = errStr.indexOf("com.gemstone.gemfire.ForcedDisconnectException") >= 0;

       // workaround BUG 40483
       boolean rootCauseIsFD = false;
       if (e instanceof GemFireException) {
          GemFireException gfe = (GemFireException)e;
          Throwable rootCause = gfe.getRootCause();
          if (rootCause instanceof ForcedDisconnectException) {
             rootCauseIsFD = true;
          }
       }
       Log.getLogWriter().info("forcedDisconnct = " + forcedDisconnect + " causedByForcedDisconnect = " + causedByForcedDisconnect + " rootCauseIsFD = " + rootCauseIsFD);
       if (!(forcedDisconnect || causedByForcedDisconnect || rootCauseIsFD)) {
          throw new TestException("doEntryOperations caught Exception " + TestHelper.getStackTrace(e));
       } else {
          if (!RemoteTestModule.getMyHost().equals(losingSideHost)) { 
             throw new TestException("doEntryOperations caught Exception " + e + " for vm " + SBUtil.getMyUniqueName() + " which should have survived the NetworkPartition");
          } else {
             SplitBrainBB.addDisconnectedClient(RemoteTestModule.getMyVmid());
          }
       }
    }

   /**
    *  CLOSETASK to check static forcedDisconect (meaning that we processed
    *  a RegionDestroyedException.FORCED_DISCONNECT in the listener.  If so,
    *  add to the list of disconnectedClients.  (We cannot do this is the 
    *  listener thread itself as the blackboard lock (to synchronize between
    *  VMs) requires a RemoteTestModule thread (and the listener thread is
    *  not one of our logical hydra task threads).
    * 
    *  Mainly used by bridgeServer VMs and gii loadClient VMs (since they do 
    *  not current have TASKS assigned).
    */
   public synchronized static void HydraCloseTask_checkForRegionDestroyedForcedDisconnects() {
      // Give ourselves a little time for the FD to come through
      MasterController.sleepForMs(30000);
      if (forcedDisconnect) {
         SplitBrainBB.addDisconnectedClient(RemoteTestModule.getMyVmid());
      }
   }

   /** 
    * ENDTASK to verify that all clients in the losing partition posted to
    * the forcedDisconnectList on the blackboard
    *
    * Also verifies proper receipt of SystemMembershipListener memberCrashed events.
    */
   public synchronized static void HydraEndTask_verifyLosingPartition() {
      Set disconnectedClients = (Set)SplitBrainBB.getForcedDisconnectList();
      Set expectedDisconnectList = (Set)SplitBrainBB.getExpectForcedDisconnects();
      StringBuffer aStr = new StringBuffer();

      if (!disconnectedClients.equals(expectedDisconnectList)) {
        aStr.append("Expected forcedDisconnects in clientVms [ " + expectedDisconnectList + " ] but the following clientVms had forcedDisconnects [" + disconnectedClients + "]\n");
      } else {
        Log.getLogWriter().info("All expected clientVms in losing partition were forcefully disconnected [ " + disconnectedClients + " ].  No other vms were forcibly disconnected\n");
      }
   
      // Verify we received the proper number of memberCrashed events 
      Set memberList = (Set)SplitBrainBB.getMembers();
      for (Iterator it=memberList.iterator(); it.hasNext(); ) {
         CrashEventMemberInfo m = (CrashEventMemberInfo)it.next();
         try {
            m.validate();
         } catch (TestException te) {
            aStr.append(te);
         }
      }
      
      // Finally, look for any Exceptions posted to the BB
      try {
         TestHelper.checkForEventError(SplitBrainBB.getBB());
      } catch (TestException te) {
         aStr.append("Listener encountered exceptions, first Exception = \n" + te);
      }
      
      if (aStr.length() > 0 ) {
         throw new TestException(aStr.toString());
      }
   }

   /** Task to verify that the region recovered data from disk (it's size is not 0)
    */
   public static void HydraTask_verifyRegion() {
      int size = aRegion.size();
      Log.getLogWriter().info(aRegion.getFullPath() + " is size " + size);           
      if (aRegion.size() == 0) {
         throw new TestException("Expected region size to be > 0, but it is " + size);
      }
      verifyFromSnapshot(aRegion, (Map) SplitBrainBB.getBB().getSharedMap().get(SNAPSHOT_KEY));
   }
   
   /** Task to add a MembershipTestHook to listen for membership failures
    */
   public static void HydraTask_addAdminFailureListener() {
     if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
       MembershipManagerHelper.addTestHook(DistributedSystemHelper.getDistributedSystem(), 
           new MembershipTestHook() {
            public void beforeMembershipFailure(String reason, Throwable cause) {
            }
            public void afterMembershipFailure(String reason, Throwable cause) {
              SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.adminForcedDisconnects);
            }
         
       });
       SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.expectedAdminForcedDisconnects);
     }
   }
   
   /** Task to verify that the correct number of admins got forced disconnects
    */
   public static void HydraTask_verifyAdminFailures() {
     String losingPartition = TestConfig.tab().stringAt(SplitBrainPrms.losingPartition);
     if ((losingPartition.equalsIgnoreCase("host1")) || (losingPartition.equalsIgnoreCase("host2"))) {
        losingSideHost = TestConfig.getInstance().getHostDescription(losingPartition).getHostName();
       if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
         SharedCounters sc = SplitBrainBB.getBB().getSharedCounters();
         long expected = sc.read(SplitBrainBB.expectedAdminForcedDisconnects);
         long actual = sc.read(SplitBrainBB.adminForcedDisconnects);
         if (expected != actual) {
           throw new TestException("Expected " + expected + " admin forced-disconnects but found " + actual);
         }
       }
     }
   }
   
   
   /** 
    * ENDTASK to wait for distributed system to reconnect
    */
   public synchronized static void HydraEndTask_waitForReconnect() {
     if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
       // wait to be reconnected
       boolean reconnected = testInstance.waitForReconnect();
       if (reconnected) {
         SplitBrainBB.addReconnectedClient(RemoteTestModule.getMyVmid());
       }
     }
   }

   /** 
    * ENDTASK to verify that all clients in the losing partition posted to
    * the reconnected bb
    */
   public synchronized static void HydraEndTask_verifyReconnect() {
     if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
        Set expectedDisconnectList = (Set)SplitBrainBB.getExpectForcedDisconnects();
        Set reconnectedClients = (Set)SplitBrainBB.getReconnectedList();
        StringBuffer aStr = new StringBuffer();
  
        // Verify that all clients reconnected
        if (!expectedDisconnectList.equals(reconnectedClients)) {
          aStr.append("Expected all clients to reconnect but only these did: " + reconnectedClients
              + ".   expected list is " + expectedDisconnectList + "\n");
        }
  
        if (aStr.length() > 0 ) {
           throw new TestException(aStr.toString());
        }
        testInstance.reconnected();
     }
   }
   
   /** Verify a region against the given expected map
    * 
    * @param aRegion The region to verify.
    * @param expectedMap The expected contents of the region.
    */
   private static void verifyFromSnapshot(Region aRegion, Map expectedMap) {
     Log.getLogWriter().info("Verifying " + aRegion.getFullPath() + " against snapshot of size " + expectedMap.size());
     StringBuffer aStr = new StringBuffer();
     int regionSize = aRegion.size();
     int expectedSize = expectedMap.size();
     if (regionSize != expectedSize) {
       aStr.append(aRegion.getFullPath() + " size is " + regionSize + " but expected it to be " + expectedSize + "\n");
     }
     Set expectedKeys = new HashSet(expectedMap.keySet());
     Set actualKeys = new HashSet(aRegion.keySet());
     Set missingKeys = new HashSet(expectedKeys);
     missingKeys.removeAll(actualKeys);
     Set extraKeys = new HashSet(actualKeys);
     extraKeys.removeAll(expectedKeys);
     if (missingKeys.size() > 0) {
       aStr.append("The following " + missingKeys.size() + " expected keys were missing from " + aRegion.getFullPath() + ": " + missingKeys + "\n");
     }
     if (extraKeys.size() > 0) {
       aStr.append("The following " + extraKeys.size() + " extra keys were found in " + aRegion.getFullPath() + ": " + extraKeys + "\n");
     }

     // now for those keys that exist in aRegion, verify their values
     for (Object key: aRegion.keySet()) {
       Object value = aRegion.get(key);
       if (expectedMap.containsKey(key)) {
         Object expectedValue = expectedMap.get(key);
         if (value instanceof BaseValueHolder) {
           value = ((BaseValueHolder)value).myValue;
         }
         if (value == null) {
           if (expectedValue != null) {
              aStr.append("For key " + key + " expectedValue is " + TestHelper.toString(expectedValue) + " but value in region is " + TestHelper.toString(value) + "\n");
           }
         } else { 
           if (!value.equals(expectedValue)) {
             aStr.append("Expected value for key " + key + " to be " + TestHelper.toString(expectedValue) + " but it is " + TestHelper.toString(value) + "\n");
           }
         }
       } else {
         aStr.append("Key " + key + " does not exist in " + aRegion.getFullPath() + "\n");
       }
     }

     if (aStr.length() > 0) {
       throw new TestException(aStr.toString());
     }
     Log.getLogWriter().info("Done verifying " + expectedMap.size() + " entries in " + aRegion.getFullPath());
   }

  /**
   * Creates a (disconnected) locator and does not throw an exception if it already exists.
   */
  public static void createLocatorTask() {
    try {
       SBUtil.createLocatorTask();
    } catch (HydraRuntimeException e) {
       Log.getLogWriter().info("NetworkPartitionTest caught " + e + " indicating locator is already created");
    }
  }

  /**
   * Connects a locator to its distributed system and does not throw an exception if already connected.
   */
  public static void startAndConnectLocatorTask() {
    try {
       SBUtil.startAndConnectLocatorTask();
    } catch (HydraRuntimeException e) {
       Log.getLogWriter().info("NetworkPartitionTest caught " + e + " indicating locator is already started");
    }
  }


}
