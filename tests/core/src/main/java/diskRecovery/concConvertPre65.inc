hydra.Prms-testRequirement = "Test pre-6.5 disk file conversion to 6.5 (or later, but < 7.0)";
hydra.Prms-testDescription = "
   The test creates 2 distributed systems: one pre-6.5 and the other 6.5 or later
   In the pre-6.5 distributed system:
      1) create regions
      2) do random ops to create data, while ops are running take down the old
         version vms one at a time so each vm will have different data on its
         disk files
   Convert pre-6.5 disk files
   In the 6.5 or later distributed system
      1) initialize with converted disk files
      2) choose a vm at random to write a snapshot to the bb, others must match it
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_4.inc;

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

// make 2 distributed systems; one with old version locator and peers and another
// with new version locator and peers
hydra.GemFirePrms-distributedSystem =
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"oldVersionDS\",    ${oldVersionLocatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"oldVersionDS\",    ${oldVersionHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"newVersionDS\",    ${newVersionLocatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"newVersionDS\",    ${newVersionHosts})"
                              ncf;

// define thread groups
// the locator threads are also used as the controller threads
THREADGROUP oldVersionLocatorThread
    totalThreads = fcn
                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;
THREADGROUP oldVersionThreads
    totalThreads = fcn
                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;
THREADGROUP newVersionLocatorThread
    totalThreads = fcn
                   ${${C}Hosts} * ${${C}VMsPerHost} * ${${C}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${C}\", ${${C}Hosts}, true)"
                   ncf;
THREADGROUP newVersionThreads 
    totalThreads = fcn
                   ${${D}Hosts} * ${${D}VMsPerHost} * ${${D}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${D}\", ${${D}Hosts}, true)"
                   ncf;

// make the old version locator and old version peers use "oldVersion" in VersionPrms-names
// make the new version locator and new version peers use "newVersion" in VersionPrms-names
hydra.ClientPrms-versionNames = fcn "hydra.TestConfigFcns.duplicate
                                     (\"oldVersion\",    ${oldVersionLocatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"oldVersion\",    ${oldVersionHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"newVersion\",    ${newVersionLocatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"newVersion\",    ${newVersionHosts})"
                                ncf;
hydra.VersionPrms-names   = oldVersion     newVersion;
hydra.VersionPrms-version = ${oldVersion}  ${newVersion};
hydra.VersionPrms-gemfireHome =
  fcn "hydra.VersionDescription.getGemFireHome(\"${oldVersion}\")" ncf
  fcn "hydra.VersionDescription.getGemFireHome(\"${newVersionHome}\")" ncf
  ;

// ************************************************************************************
// Initialization tasks

// initialize for stop/start
INITTASK     taskClass     = util.StopStartVMs  taskMethod = StopStart_initTask
             threadGroups = oldVersionThreads, newVersionThreads;

// start the locators for the old and new versions
INITTASK     taskClass   = hydra.DistributedSystemHelper    taskMethod = createLocator
             threadGroups = oldVersionLocatorThread, newVersionLocatorThread;
INITTASK     taskClass   = hydra.DistributedSystemHelper    taskMethod = startLocatorAndDS
             threadGroups = oldVersionLocatorThread, newVersionLocatorThread;

// initialize the oldVersion jvms 
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = oldVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createXmlFile
             threadGroups = oldVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createExtraRegions
             threadGroups = oldVersionThreads;

// prepare for disk file conversion in the newVersion jvms 
// this creates new version xml files to be used by the conversion tool
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createExtraRegions
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createXmlFile
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_disconnect
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTestVersionHelper  taskMethod = removeDiskFiles
             threadGroups = newVersionThreads;

// ************************************************************************************
// oldVersion tasks

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = newVersionThreads
             runMode = dynamic;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_verifyFromLeaderSnapshot
             threadGroups = newVersionThreads
             runMode = dynamic;

TASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_doOperations
         threadGroups = oldVersionThreads;

TASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_convertPre65Controller
         maxTimesToRun = 1
         threadGroups = newVersionLocatorThread;

// ************************************************************************************
// newVersion close tasks

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_disconnect
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = util.PersistenceUtil  taskMethod = HydraTask_initialize
             threadGroups = newVersionLocatorThread;

CLOSETASK    taskClass   = util.PersistenceUtil  taskMethod = doOfflineValAndCompaction
             threadGroups = newVersionLocatorThread;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_waitForMyStartupRecovery
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_verifyPRs
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_verifyFromSnapshot
             threadGroups = newVersionThreads;
  
hydra.Prms-totalTaskTimeSec = 180;
hydra.Prms-maxResultWaitSec = 600;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = RANGE 1 50 EGNAR;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

// ************************************************************************************
hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

// ************************************************************************************

diskRecovery.RecoveryPrms-operations = ONEOF add add destroy destroy invalidate invalidate update update get get getNew getNew 
                                             putAll putAll cacheOperations cacheOperations FOENO;
diskRecovery.RecoveryPrms-lowerThreshold = 10;
diskRecovery.RecoveryPrms-upperThreshold = 100;
diskRecovery.RecoveryPrms-lowerThresholdOperations = ONEOF add putAll add putAll cacheOperations FOENO;
diskRecovery.RecoveryPrms-upperThresholdOperations = ONEOF destroy cacheOperations FOENO;
diskRecovery.RecoveryPrms-numPutAllNewKeys = RANGE 1 10 EGNAR;
diskRecovery.RecoveryPrms-numPutAllExistingKeys = RANGE 1 10 EGNAR;
diskRecovery.RecoveryPrms-useUniqueKeys = true;
diskRecovery.RecoveryPrms-convertWithNewVersionXml = ${convertWithNewVersionXml};

util.TestHelperPrms-minTaskGranularitySec = 15;

util.StopStartPrms-stopModes = ONEOF nice_exit mean_exit mean_kill FOENO;

